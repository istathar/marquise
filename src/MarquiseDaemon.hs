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

    parseCacheFlushFreq = O.option $
           long "cache-flush-freq"
        <> short 't'
        <> help "Period of time to wait between cache writes"
        <> value 42

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument str (metavar "ORIGIN")

defaultCacheLoc :: String -> String
defaultCacheLoc o = "/var/spool/marquise/source_dict_hash_cache_" ++ o

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
