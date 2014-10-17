--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | Marquise client interface for sending data to the vault.
--
-- This module provides functions for preparing and queuing points to be sent
-- by a Marquise server to the vault.
--
-- If you call close, you can be assured that your data is safe and will at
-- some point end up in the data vault (excluding local disk failure). This
-- assumption is based on a functional marquise daemon with connectivity
-- eventually running within your namespace.
--
-- We provide no way to *absolutely* ensure that a point is currently written
-- to the vault. Such a guarantee would require blocking and complex queuing,
-- or observing various underlying mechanisms that should ideally remain
-- abstract.
--

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFunctor #-}

-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.Client
(
    -- | * Utility functions
    -- Note: You may read MarquiseSpoolFileMonad m as IO.
    hashIdentifier,
    makeSpoolName,
    makeOrigin,

    -- | * Contents daemon requests
    withContentsConnection,
    withContentsConnectionT,
    requestUnique,
    makeSourceDict,
    updateSourceDict,
    removeSourceDict,
    enumerateOrigin,
    enumerateOriginResume,

    -- | * Queuing data to be sent to vaultaire
    createSpoolFiles,
    queueSimple,
    queueExtended,
    queueSourceDictUpdate,
    flush,

    -- | Reading from Vaultaire
    withReaderConnection,
    withReaderConnectionT,
    readExtended,
    readExtendedPoints,
    readExtendedPointsResume,
    readSimple,
    readSimplePoints,
    readSimplePointsResume,
    decodeExtended,
    decodeSimple,

    -- * Types
    SourceDict,
    SpoolName,
    SpoolFiles,
    Address(..),
    Origin(..),
    TimeStamp(..),
    SimpleBurst(..),
    SimplePoint(..),
    SocketState(..),
) where

import           Control.Applicative
import           Control.Monad.Error
import           Control.Monad.State.Strict
import           Crypto.MAC.SipHash
import           Data.Bits
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Char (isAlphaNum)
import           Data.Packer
import           Data.Word (Word64)
import qualified Data.HashSet  as HS
import           Pipes
import qualified Pipes.Prelude as P

import           Marquise.Classes
--import           Marquise.Client.IO ()
import           Marquise.IO.Connection
import           Marquise.Types
import           Vaultaire.Types


-- | Create a SpoolName. Only alphanumeric characters are allowed, max length
-- is 32 characters.
makeSpoolName :: Monad m => String -> Marquise m SpoolName
makeSpoolName s
    | any (not . isAlphaNum) s = throwError $ InvalidSpoolName s
    | otherwise                = return (SpoolName s)

-- | Create a name in the spool. Only alphanumeric characters are allowed, max length
-- is 32 characters.
createSpoolFiles :: MarquiseSpoolFileMonad m
                 => String
                 -> Marquise m SpoolFiles
createSpoolFiles s = do
  n <- makeSpoolName s
  createDirectories n
  randomSpoolFiles  n

-- | Deterministically convert a ByteString to an Address by taking the
-- most significant 63 bytes of its SipHash-2-4[0] with a zero key. The
-- LSB of the resulting 64-bit value is not part of the unique portion
-- of the address; it is set when queueing writes, depending on the
-- point type (simple or extended) being written.
--
-- [0] https://131002.net/siphash/
hashIdentifier :: ByteString -> Address
hashIdentifier = Address . (`clearBit` 0) . unSipHash . hash iv
  where
    iv = SipKey 0 0
    unSipHash (SipHash h) = h :: Word64

-- | Generate an un-used Address. You will need to store this for later re-use.
requestUnique :: MarquiseContentsMonad m conn
               => Origin
               -> conn
               -> Marquise m Address
requestUnique origin conn =  do
    sendContentsRequest GenerateNewAddress origin conn
    response <- recvContentsResponse conn
    case response of
        RandomAddress addr -> return addr
        _ -> error "requestUnique: Invalid response"

-- | Set the key,value tags as metadata on the given Address.
updateSourceDict :: MarquiseContentsMonad m conn
                 => Address
                 -> SourceDict
                 -> Origin
                 -> conn
                 -> Marquise m ()
updateSourceDict addr source_dict origin conn =  do
    sendContentsRequest (UpdateSourceTag addr source_dict) origin conn
    response <- recvContentsResponse conn
    case response of
        UpdateSuccess -> return ()
        _ -> error "requestSourceDictUpdate: Invalid response"

-- | Remove the supplied key,value tags from metadata on the Address, if present.
removeSourceDict :: MarquiseContentsMonad m conn
                 => Address
                 -> SourceDict
                 -> Origin
                 -> conn
                 -> Marquise m ()
removeSourceDict addr source_dict origin conn = do
    sendContentsRequest (RemoveSourceTag addr source_dict) origin conn
    response <- recvContentsResponse conn
    case response of
        RemoveSuccess -> return ()
        _ -> error "requestSourceDictRemoval: Invalid response"

-- | Stream read every Address associated with the given Origin
enumerateOrigin :: MarquiseContentsMonad m conn
                => Origin
                -> conn
                -> Producer (Address, SourceDict) (Marquise m) ()
enumerateOrigin origin conn = do
    lift $ sendContentsRequest ContentsListRequest origin conn
    loop
  where
    loop = do
        resp <- lift $ recvContentsResponse conn
        case resp of
            ContentsListEntry addr dict -> do
                yield (addr, dict)
                -- add the new address to this operation's error state
                -- so it can be ignored if the operation is resumed.
                lift $ get >>= put . EnumOrigin . HS.insert addr . enumerated
                loop
            EndOfContentsList -> return ()
            _ -> error "enumerateOrigin loop: Invalid response"

-- | Like @enumerateOrigin@, but also returns a resumption pipe
--   for the user to run and get the rest of the addresses in a consistent manner
--   (i.e. any addresses already yielded will be ignored)
--
enumerateOriginResume :: MarquiseContentsMonad m conn
                      => Origin
                      -> conn
                      -> Result (Address, SourceDict) (Marquise m)
enumerateOriginResume origin conn = Result $ catchMarquiseP
    (enumerateOrigin origin conn >> return Nothing)
    (\x -> case x of
      (Timeout      (EnumOrigin seen))   -> _result (enumerateOriginResume origin conn) >-> stop seen
      (ZMQException (EnumOrigin seen) _) -> _result (enumerateOriginResume origin conn) >-> stop seen
      _                                  -> throwError x)
    where stop x = P.filter (flip HS.member x . fst)

-- | Stream read every SimpleBurst from the Address between the given times
readSimple :: MarquiseReaderMonad m conn
           => Address
           -> TimeStamp
           -> TimeStamp
           -> Origin
           -> conn
           -> Producer' SimpleBurst (Marquise m) ()
readSimple addr start end origin conn = do
    lift $ sendReaderRequest (SimpleReadRequest addr start end) origin conn
    loop
  where
    loop = do
        response <- lift $ recvReaderResponse conn
        case response of
            SimpleStream burst ->
                yield burst >> loop
            EndOfStream ->
                return ()
            InvalidReadOrigin ->
                error "readSimple loop: Invalid origin"
            _ ->
                error "readSimple loop: Invalid response"

-- | Like @readSimple@ but also decodes the points.
--
readSimplePoints
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Producer' SimplePoint (Marquise m) ()
readSimplePoints addr start end origin conn
    = for (readSimple addr start end origin conn >-> decodeSimple)
          (\point -> do put $ ReadPoints $ simpleTime point
                        yield point)

-- | Like @readSimplePoints@, but also returns a resumption pipe that we can run and
--   get the rest of the result.
--
readSimplePointsResume
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Result SimplePoint (Marquise m)
readSimplePointsResume addr start end origin conn = Result $ catchMarquiseP
    (readSimplePoints addr start end origin conn >> return Nothing)
    (\x -> case x of
      -- to resume, read from the last point yielded (exclusive)
      Timeout (ReadPoints t)        -> _result (ignoreFirst $ readSimplePointsResume addr t end origin conn)
      ZMQException (ReadPoints t) _ -> _result (ignoreFirst $ readSimplePointsResume addr t end origin conn)
      _                             -> throwError x)

-- | Stream read every ExtendedBurst from the Address between the given times
readExtended :: MarquiseReaderMonad m conn
             => Address
             -> TimeStamp
             -> TimeStamp
             -> Origin
             -> conn
             -> Producer' ExtendedBurst (Marquise m) ()
readExtended addr start end origin conn = do
    lift $ sendReaderRequest (ExtendedReadRequest addr start end) origin conn
    loop
  where
    loop = do
        response <- lift $ recvReaderResponse conn
        case response of
            ExtendedStream burst ->
                yield burst >> loop
            EndOfStream ->
                return ()
            _ ->
                error "readExtended loop: Invalid response"

-- | Like @readExtended@ but also decodes the points.
--
readExtendedPoints
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Producer' ExtendedPoint (Marquise m) ()
readExtendedPoints addr start end origin conn
    = for (readExtended addr start end origin conn >-> decodeExtended)
          (\point -> do put $ ReadPoints $ extendedTime point
                        yield point)

-- | Like @readExtendedPoints@, but also returns a resumption pipe that we can run and
--   get the rest of the result.
--
readExtendedPointsResume
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Result ExtendedPoint (Marquise m)
readExtendedPointsResume addr start end origin conn = Result $ catchMarquiseP
    (readExtendedPoints addr start end origin conn >> return Nothing)
    (\x -> case x of
      -- to resume, read from the last point yielded (exclusive)
      Timeout (ReadPoints t)        -> _result (ignoreFirst $ readExtendedPointsResume addr t end origin conn)
      ZMQException (ReadPoints t) _ -> _result (ignoreFirst $ readExtendedPointsResume addr t end origin conn)
      _                             -> throwError x)

-- | Stream converts raw SimpleBursts into SimplePoints
decodeSimple :: Monad m => Pipe SimpleBurst SimplePoint (Marquise m) ()
decodeSimple = forever (unSimpleBurst <$> await >>= emitFrom 0)
  where
    emitFrom os chunk
        | os >= BS.length chunk = return ()
        | otherwise = do
            yield $ flip runUnpacking chunk $ do
                unpackSetPosition os
                addr <- Address <$> getWord64LE
                time <- TimeStamp <$> getWord64LE
                payload <- getWord64LE
                return $ SimplePoint addr time payload

            emitFrom (os + 24) chunk


-- | Stream converts raw ExtendedBursts into ExtendedPoints
decodeExtended :: Monad m => Pipe ExtendedBurst ExtendedPoint m ()
decodeExtended = forever (unExtendedBurst <$> await >>= emitFrom 0)
  where
    emitFrom os chunk
        | os >= BS.length chunk = return ()
        | otherwise = do
            let result = runUnpacking (unpack os) chunk
            yield result

            let size = BS.length (extendedPayload result) + 24
            emitFrom (os + size) chunk

    unpack os = do
        unpackSetPosition os
        addr <- Address <$> getWord64LE
        time <- TimeStamp <$> getWord64LE
        len <- fromIntegral <$> getWord64LE
        payload <- if len == 0
                       then return BS.empty
                       else getBytes len

        return $ ExtendedPoint addr time payload

-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
-- Clears the least-significant bit of the address to indicate that this
-- is a simple datapoint.
queueSimple
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> TimeStamp
    -> Word64
    -> Marquise m ()
queueSimple sfs (Address ad) (TimeStamp ts) w = appendPoints sfs bytes
  where
    bytes = runPacking 24 $ do
        putWord64LE (ad `clearBit` 0)
        putWord64LE ts
        putWord64LE w

-- | Send an "extended" data point. Again, representation is up to you.
-- Sets the least-significant bit of the address to indicate that this is
-- an extended data point.
queueExtended
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> TimeStamp
    -> ByteString
    -> Marquise m ()
queueExtended sfs (Address ad) (TimeStamp ts) bs = appendPoints sfs bytes
  where
    len = BS.length bs
    bytes = runPacking (24 + len) $ do
        putWord64LE (ad `setBit` 0)
        putWord64LE ts
        putWord64LE $ fromIntegral len
        putBytes bs

-- | Updates the SourceDict at addr with source_dict
queueSourceDictUpdate
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> SourceDict
    -> Marquise m ()
queueSourceDictUpdate sfs (Address addr) source_dict = appendContents sfs bytes
  where
    source_dict_bytes = toWire source_dict
    source_dict_len = BS.length source_dict_bytes
    bytes = runPacking (source_dict_len + 16) $ do
        putWord64LE addr
        putWord64LE (fromIntegral source_dict_len)
        putBytes source_dict_bytes

-- | Ensure that all sent points have hit the local disk.
flush
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Marquise m ()
flush = close
