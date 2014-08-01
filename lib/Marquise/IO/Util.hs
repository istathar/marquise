--
-- Copyright © 2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | Utility module for IO specific reader operations

{-# LANGUAGE RankNTypes #-}

module Marquise.IO.Util(
    consistentEnumerateOrigin,
    consistifyReader,
    chunkifyReader,
    consistentReadSimple,
    consistentReadExtended,
    chunkedReadSimple,
    chunkedReadExtended
) where

import Control.Exception
import Pipes
import qualified Pipes.Prelude as Pipes
import System.IO

import Marquise.Client
import Marquise.Types
import Vaultaire.Types

-- | Convenience type synonym for readers in IO
type IOReader a = Address -> TimeStamp -> TimeStamp -> Origin -> SocketState -> Producer' a IO ()

-- | Consistent version of enumerateOrigin in IO
consistentEnumerateOrigin :: Origin
                          -> SocketState
                          -> Producer' (Address, SourceDict) IO ()
consistentEnumerateOrigin origin conn = do
    output <- liftIO $ getPairs
    each output
  where
    getPairs = catch (Pipes.toListM $ enumerateOrigin origin conn) handleOriginTimeout
    handleOriginTimeout e = do
        hPutStrLn stderr $ concat ["Caught: ", Prelude.show (e :: MarquiseTimeout), " restarting enumerateOrigin."]
        getPairs

-- | Chunked and consistent versions of the readSimple and readExtended functions
-- These versions handle timeouts and guarantee consistency of data in IO

chunkedReadSimple      :: Int -> IOReader SimpleBurst
chunkedReadSimple      = chunkifyReader readSimple

chunkedReadExtended    :: Int -> IOReader ExtendedBurst
chunkedReadExtended    = chunkifyReader readExtended

consistentReadSimple   :: IOReader SimpleBurst
consistentReadSimple   = consistifyReader readSimple

consistentReadExtended :: IOReader ExtendedBurst
consistentReadExtended = consistifyReader readExtended

-- | Converts a reader to a consistent version
consistifyReader :: IOReader a
                 -> IOReader a
consistifyReader readerFunc = chunkifyReader readerFunc 1

-- | Helper to chunk a reader in the IO Monad. Guarantees non-duplicated
-- output on timeouts. Note: uses non-constant memory
chunkifyReader :: IOReader a
               -> Int
               -> IOReader a
chunkifyReader readerFunc numChunks addr start end origin conn
    | numChunks == 0 = error "Cannot chunk reader into 0 chunks"
    | numChunks == 1 = readerFunc addr start end origin conn
    | otherwise = do
        let timePerChunk = (end - start) `div` (fromIntegral numChunks)
        let starts = take numChunks [start, start + timePerChunk..]
        let ends = (map (\x -> x - 1) (tail starts)) ++ [end]
        chunkifyReader' readerFunc addr starts ends origin conn

-- | Helper function for chunkifyReader
chunkifyReader' :: IOReader a
                -> Address
                -> [TimeStamp]
                -> [TimeStamp]
                -> Origin
                -> SocketState
                -> Producer' a IO ()
chunkifyReader' _ _ [] _ _ _ = return ()
chunkifyReader' _ _ _ [] _ _ = return ()
chunkifyReader' readerFunc addr (start:starts) (end:ends) origin conn = do
    results <- liftIO $ readChunk readerFunc addr start end origin conn
    yieldAll results
 where
    yieldAll [] = chunkifyReader' readerFunc addr starts ends origin conn
    yieldAll (x:xs) = yield x >> yieldAll xs

-- | Reads a single 'chunk' handling timeout exceptions
readChunk :: IOReader a
          -> Address
          -> TimeStamp
          -> TimeStamp
          -> Origin
          -> SocketState
          -> IO [a]
readChunk readerFunc addr start end origin conn =
    catch (Pipes.toListM $ readerFunc addr start end origin conn)
            (handleTimeout readerFunc addr start end origin conn)

-- | Handles a caught timeout exception and attempts to read again
handleTimeout :: IOReader a
              -> Address
              -> TimeStamp
              -> TimeStamp
              -> Origin
              -> SocketState
              -> MarquiseTimeout
              -> IO [a]
handleTimeout readerFunc addr start end origin conn e = do
    hPutStrLn stderr $ concat ["Caught: ", Prelude.show e, " restarting read for chunk."]
    readChunk readerFunc addr start end origin conn
