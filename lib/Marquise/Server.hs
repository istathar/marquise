{-# LANGUAGE MultiParamTypeClasses #-}
--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    marquiseServer,
    parseContentsRequests,
    breakInToChunks,
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Exception (throw, throwIO)
import Control.Monad
import Data.Attoparsec.ByteString.Lazy (Parser)
import Data.Attoparsec.Combinator (eitherP)
import qualified Data.Attoparsec.Lazy as Parser
import Data.ByteString.Builder (Builder, byteString, toLazyByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Data.Monoid
import Data.Packer
import Marquise.Classes
import Marquise.Client (makeSpoolName, updateSourceDict)
import Marquise.Types (SpoolName (..))
import Pipes
import Pipes.Attoparsec (parsed)
import qualified Pipes.ByteString as PB
import Pipes.Group (FreeF (..), FreeT (..))
import qualified Pipes.Group as PG
import System.Log.Logger
import Vaultaire.Types
import Vaultaire.Util

data ContentsRequest = ContentsRequest Address SourceDict
  deriving Show

marquiseServer :: String -> Origin -> String -> IO ()
marquiseServer broker origin user_sn =
    case makeSpoolName user_sn of
        Left e -> throwIO e
        Right sn -> do
            debugM "marquiseServer" "Creating spool directories"
            createDirectories sn
            debugM "marquiseServer" "Starting point transmitting thread"
            linkThread (sendPoints broker origin sn)
            debugM "marquiseServer" "Starting contents transmitting thread"
            linkThread (sendContents broker origin sn)
            waitForever

sendPoints :: String -> Origin -> SpoolName -> IO ()
sendPoints broker origin sn = forever $ do
    debugM "sendPoints" "Waiting for points"
    (bytes, seal) <- nextPoints sn

    debugM "sendPoints" "Got points, starting transmission pipe"
    runEffect $ for (breakInToChunks bytes) sendChunk

    debugM "sendPoints" "Transmission complete, cleaning up"
    seal

    threadDelay idleTime
  where
    sendChunk chunk = do
        let size = show . S.length $ chunk
        liftIO (debugM "sendPoints" $ "Sending chunk of " ++ size ++ " bytes")
        lift (transmitBytes broker origin chunk)

sendContents :: String
             -> Origin
             -> SpoolName
             -> IO ()
sendContents broker origin sn = forever $ do
    debugM "sendContents" "Waiting for contents"
    (bytes, seal) <- nextContents sn
    debugM "sendContents" "Got contents, starting transmission pipe"
    withContentsConnection broker $ \c ->
        runEffect $ for (parseContentsRequests bytes)
                        (sendSourceDictUpdate c)
    seal
  where
    sendSourceDictUpdate conn (ContentsRequest addr source_dict) = do
        liftIO (debugM "sendContents" $ "Sending contents update for " ++ show addr)
        lift (updateSourceDict addr source_dict origin conn)

parseContentsRequests :: Monad m => L.ByteString -> Producer ContentsRequest m ()
parseContentsRequests bs =
    parsed parseContentsRequest (PB.fromLazy bs)
    >>= either (throw . fst) return

parseContentsRequest :: Parser ContentsRequest
parseContentsRequest = do
    addr <- fromWire <$> Parser.take 8
    len <- runUnpacking getWord64LE <$> Parser.take 8
    source_dict <- fromWire <$> Parser.take (fromIntegral len)
    case ContentsRequest <$> addr <*> source_dict of
        Left e -> fail (show e)
        Right request -> return request

idleTime :: Int
idleTime = 1000000 -- 1 second

breakInToChunks :: Monad m => L.ByteString -> Producer S.ByteString m ()
breakInToChunks bs =
    chunkBuilder (parsed parsePoint (PB.fromLazy bs))
    >>= either (throw . fst) return

-- Take a producer of (Int, Builder), where Int is the number of bytes in the
-- builder and produce chunks of n bytes.
--
-- This could be done with explicit recursion and next, but, then we would not
-- get to apply a fold over a FreeT stack of producers. This is almost
-- generalizable, at a stretch.
chunkBuilder :: Monad m => Producer (Int, Builder) m r -> Producer S.ByteString m r
chunkBuilder = PG.folds (<>) mempty (L.toStrict . toLazyByteString)
             -- ^ Fold over each producer of counted Builders, turning it into
             --   a contigous strict ByteString ready for transmission.
             . builderChunks idealBurstSize
             -- ^ Split the builder producer into FreeT
  where
    builderChunks :: Monad m
                  => Int
                  -- ^ The size to split a stream of builders at
                  -> Producer (Int, Builder) m r
                  -- ^ The input producer
                  -> FreeT (Producer Builder m) m r
                  -- ^ The FreeT delimited chunks of that producer, split into
                  --   the desired chunk length
    builderChunks max_size p = FreeT $ do
        -- Try to grab the next value from the Producer
        x <- next p
        return $ case x of
            Left r -> Pure r
            Right (a, p') -> Free $ do
                -- Pass the re-joined Producer to go, which will yield values
                -- from it until the desired chunk size is reached.
                p'' <- go max_size (yield a >> p')
                -- The desired chunk size has been reached, loop and try again
                -- with the rest of the stream (possibly empty)
                return (builderChunks max_size p'')

    -- We take a Producer and pass along its values until we've passed along
    -- enough bytes (at least the initial bytes_left).
    --
    -- When done, returns the remainder of the unconsumed Producer
    go :: Monad m
       => Int
       -> Producer (Int, Builder) m r
       -> Producer Builder m (Producer (Int, Builder) m r)
    go bytes_left p =
        if bytes_left < 0
            then return p
            else do
                x <- lift (next p)
                case x of
                    Left r ->
                        return . return $ r
                    Right ((size, builder), p') -> do
                        yield builder
                        go (bytes_left - size) p'

-- Parse a single point, returning the size of the point and the bytes as a
-- builder.
parsePoint :: Parser (Int, Builder)
parsePoint = do
    packet <- Parser.take 24

    case extendedSize packet of
        Just len -> do
            -- We must ensure that we get this many bytes now, or attoparsec
            -- will just backtrack on us. We do this with a dummy parser inside
            -- an eitherP
            --
            -- This is only to get good error messages.
            extended <- eitherP (Parser.take len) (return ())
            case extended of
                Left bytes ->
                    let b = byteString packet <> byteString bytes
                    in return (24 + len, b)
                Right () ->
                    fail "not enough bytes in alleged extended burst"
        Nothing ->
            return (24, byteString packet)

-- Return the size of the extended segment, if the point is an extended one.
extendedSize :: S.ByteString -> Maybe Int
extendedSize packet = flip runUnpacking packet $ do
    addr <- Address <$> getWord64LE
    if isAddressExtended addr
        then do
            unpackSkip 8
            Just . fromIntegral <$> getWord64LE -- length
        else
            return Nothing

-- A burst should be, at maximum, very close to this side, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Int
idealBurstSize = 16 * 1048576
