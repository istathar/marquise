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

{-# LANGUAGE MultiParamTypeClasses #-}

-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    runMarquiseDaemon,
    parseContentsRequests,
    breakInToChunks,
)
where

import           Control.Applicative
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async
import qualified Control.Concurrent.Async.Lifted as AL
import           Control.Concurrent.MVar
import           Control.Exception (throw)
import           Control.Monad
import           Control.Monad.Error
import           Control.Monad.State.Lazy
import           Data.Attoparsec.ByteString.Lazy (Parser)
import           Data.Attoparsec.Combinator (eitherP)
import qualified Data.Attoparsec.Lazy as Parser
import           Data.ByteString.Builder (Builder, byteString, toLazyByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import           Data.Maybe
import           Data.Monoid
import           Data.Packer
import           Data.Time.Clock
import           Pipes
import           Pipes.Attoparsec (parsed)
import qualified Pipes.ByteString as PB
import           Pipes.Group (FreeF (..), FreeT (..))
import qualified Pipes.Group as PG
import qualified Pipes.Lift  as P
import           System.IO
import           System.Log.Logger

import           Vaultaire.Types
import           Marquise.Classes
import           Marquise.Client (makeSpoolName, updateSourceDict)
import           Marquise.Types
import           Marquise.IO ()

data ContentsRequest = ContentsRequest Address SourceDict
  deriving Show

runMarquiseDaemon :: String -> Origin -> String -> MVar () -> String -> Integer -> IO (Async ())
runMarquiseDaemon broker origin namespace shutdown cache_file cache_flush_period =
  async $ handleErrors
        $ unMarquise'
        $ startMarquise broker origin namespace shutdown cache_file cache_flush_period
  where handleErrors :: IO (Either MarquiseErrorType (), ErrorState) -> IO ()
        handleErrors act = act >>= (either catchThemAll return) . fst
        catchThemAll e = case e of
          -- We can define a "timeout policy" as a config option, i.e. whether to restart
          -- the marquise daemon automatically.
          (Timeout _)        -> error $ show e
          -- likewise automatic handling of some errors, such as zmq errors can be defined as
          -- config options
          _                  -> error $ show e


startMarquise :: String -> Origin -> String -> MVar () -> String -> Integer -> Marquise IO ()
startMarquise broker origin name shutdown cache_file cache_flush_period = do
    catchTryIO $ infoM "Server.startMarquise" $ "Reading SourceDict cache from " ++ cache_file
    init_cache <- catchTryIO $ withFile cache_file ReadWriteMode $ \h -> do
        result <- fromWire <$> S.hGetContents h
        case result of
            Left e -> do
                warningM "Server.startMarquise" $
                    concat ["Error decoding hash file: "
                           , show e
                           , " Continuing with empty initial cache"
                           ]
                return emptySourceCache
            Right cache -> do
                debugM "Server.startMarquise" $
                    concat ["Read "
                           , show (sizeOfSourceCache cache)
                           , " hashes from source dict cache."
                           ]
                return cache
    catchTryIO $ infoM "Server.startMarquise" "Marquise daemon started"

    (points_loop, final_cache) <- do
        sn <- makeSpoolName name
        catchTryIO $ debugM "Server.startMarquise" "Creating spool directories"
        createDirectories sn
        catchTryIO $ debugM "Server.startMarquise" "Starting point transmitting thread"
        points_loop <- AL.async (sendPoints broker origin sn shutdown)
        currTime <- catchTryIO $ do
          link points_loop
          debugM "Server.startMarquise" "Starting contents transmitting thread"
          getCurrentTime
        final_cache <- sendContents broker origin sn init_cache cache_file cache_flush_period currTime shutdown
        return (points_loop, final_cache)

    catchTryIO $ do
      debugM "Server.startMarquise" "Send loop shut down gracefully, writing out cache"
      S.writeFile cache_file $ toWire final_cache

    catchTryIO $ debugM "Server.startMarquise" "Waiting for points loop thread"
    AL.wait points_loop

sendPoints :: String -> Origin -> SpoolName -> MVar () -> Marquise IO ()
sendPoints broker origin sn shutdown = do
    nexts <- nextPoints sn
    case nexts of
        Just (bytes, seal) -> do
            catchTryIO $ debugM "Server.sendPoints" "Got points, starting transmission pipe"
            runEffect $ for (breakInToChunks bytes) sendChunk
            catchTryIO $ do
              debugM "Server.sendPoints" "Transmission complete, cleaning up"
              seal
        Nothing -> catchTryIO $ threadDelay idleTime

    done <- catchTryIO $ isJust <$> tryReadMVar shutdown
    unless done (sendPoints broker origin sn shutdown)
  where
    sendChunk chunk = do
        let size = show . S.length $ chunk
        liftIO (debugM "Server.sendPoints" $ "Sending chunk of " ++ size ++ " bytes.")
        lift (transmitBytes broker origin chunk)

sendContents :: String
             -> Origin
             -> SpoolName
             -> SourceDictCache
             -> String
             -> Integer
             -> UTCTime
             -> MVar ()
             -> Marquise IO SourceDictCache
sendContents broker origin sn initial cache_file cache_flush_period flush_time shutdown = do
        nexts <- nextContents sn
        (final, newFlushTime) <- case nexts of
            Just (bytes, seal) ->  do
                catchTryIO $ debugM "Server.sendContents" $
                    concat
                        [ "Got contents, starting transmission pipe with "
                        , show $ sizeOfSourceCache initial
                        , " cached sources."
                        ]

                ((), final') <- withContentsConnectionT broker $ \c ->
                    runEffect $ for (P.runStateP initial (parseContentsRequests bytes >-> filterSeen))
                                    (sendSourceDictUpdate c)

                newFlushTime' <- catchTryIO $ do
                  debugM "Server.sendContents" "Contents transmission complete, cleaning up."
                  debugM "Server.sendContents" $
                      concat
                          [ "Saw "
                          , show $ (sizeOfSourceCache final') - (sizeOfSourceCache initial)
                          , " new sources."
                          ]
                  seal
                  currTime <- getCurrentTime
                  if currTime > flush_time
                      then do
                          debugM "Server.setContents" "Performing periodic cache writeout."
                          S.writeFile cache_file $ toWire final'
                          return $ addUTCTime (fromInteger cache_flush_period) currTime
                      else do
                          debugM "Server.sendContents" $ concat ["Next cache flush at ", show flush_time, "."]
                          return flush_time
                return (final', newFlushTime')

            Nothing -> catchTryIO $ do
                threadDelay idleTime
                return (initial, flush_time)

        done <- catchTryIO $ isJust <$> tryReadMVar shutdown

        if done
        then return final
        else sendContents broker origin sn final cache_file cache_flush_period newFlushTime shutdown

  where
    filterSeen = forever $ do
        req@(ContentsRequest addr sd) <- await
        cache <- get
        let currHash = hashSource sd
        if memberSourceCache currHash cache then
            liftIO $ debugM "Server.filterSeen" $ "Seen source dict with address " ++ show addr ++ " before, ignoring."
        else do
            put (insertSourceCache currHash cache)
            yield req
    sendSourceDictUpdate conn (ContentsRequest addr source_dict) = do
        liftIO (debugM "Server.sendContents" $ "Sending contents update for " ++ show addr)
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
             -- Fold over each producer of counted Builders, turning it into
             -- a contigous strict ByteString ready for transmission.
             . builderChunks idealBurstSize
             -- Split the builder producer into FreeT
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

-- A burst should be, at maximum, very close to this size, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Int
idealBurstSize = 16 * 1048576
