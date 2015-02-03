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

{-# LANGUAGE RecordWildCards #-}

module Main where

import Control.Concurrent.Async
import qualified Data.ByteString.Char8 as S
import Data.Monoid
import Options.Applicative hiding (Parser, option)
import Options.Applicative.Types
import qualified Options.Applicative as O
import System.Log.Logger

import Marquise.Client
import Marquise.Server
import Package (package, version)
import Vaultaire.Program

data Options = Options
  { broker         :: String
  , debug          :: Bool
  , quiet          :: Bool
  , cacheFile      :: String
  , cacheFlushFreq :: Integer
  , origin         :: String
  , namespace      :: String }

justRead :: Read a => ReadM a
justRead = readerAsk >>= return . read

helpfulParser :: O.ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: O.Parser Options
optionsParser = Options <$> parseBroker
                        <*> parseDebug
                        <*> parseQuiet
                        <*> parseCacheFile
                        <*> parseCacheFlushFreq
                        <*> parseOrigin
                        <*> parseNameSpace
  where
    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value "localhost"
        <> showDefault
        <> help "Vault broker host name or IP address"

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Output lots of debugging information"

    parseQuiet = switch $
           long "quiet"
        <> short 'q'
        <> help "Only emit warnings or fatal messages"

    parseCacheFile = strOption $
           long "cache-file"
        <> short 'c'
        <> value ""
        <> help "Location to read/write cached SourceDicts"

    -- This doesn't mean that marquised will rigorously flush the cache
    -- every `t` seconds. To clarify: marquised consideres flushing the
    -- cache after it finishes reading every spool file. If the cache
    -- hasn't been flushed in the last `t` seconds, it will be flushed
    -- before processing the next spool file.
    parseCacheFlushFreq = O.option justRead $
           long "cache-flush-freq"
        <> short 't'
        <> help "Period of time to wait between cache writes, in seconds"
        <> value 42

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument str (metavar "ORIGIN")

defaultCacheLoc :: String -> String
defaultCacheLoc = (++) "/var/cache/marquise/"

main :: IO ()
main = do
    Options{..} <- execParser $ helpfulParser

    let level
          | debug     = Debug
          | quiet     = Quiet
          | otherwise = Normal

    quit <- initializeProgram (package ++ "-" ++ version) level

    cacheFile' <- return $ case cacheFile of
        "" -> defaultCacheLoc origin
        x  -> x

    a <- runMarquiseDaemon broker (Origin $ S.pack origin) namespace quit cacheFile' cacheFlushFreq

    -- wait forever
    wait a
    debugM "Main.main" "End"
