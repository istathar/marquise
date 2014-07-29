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

{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DoAndIfThenElse #-}

module Main where

import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as S
import Data.String
import Data.Time
import Data.Word (Word64)
import Options.Applicative
import Pipes
import System.Locale
import System.Log.Logger

import Marquise.Client
import Package (package, version)
import Vaultaire.Util
import Vaultaire.Program

--
-- Component line option parsing
--

data Options = Options
  { broker    :: String
  , debug     :: Bool
  , component :: Component }

data Component = 
                 Time
               | Read { origin  :: Origin
                      , address :: Address
                      , start   :: Word64
                      , end     :: Word64 }
               | List { origin :: Origin }

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parseBroker
                        <*> parseDebug
                        <*> parseComponents
  where
    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value "localhost"
        <> showDefault
        <> help "Vaultaire broker hostname or IP address"

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Output lots of debugging information"

    parseComponents = subparser
       (parseTimeComponent <> parseReadComponent <> parseListComponent)

    parseTimeComponent =
        componentHelper "now" (pure Time) "Display the current time"

    parseReadComponent =
        componentHelper "read" readOptionsParser "Read points from a given address and time range"

    parseListComponent =
        componentHelper "list" listOptionsParser "List addresses and metadata in origin"

    componentHelper cmd_name parser desc =
        command cmd_name (info (helper <*> parser) (progDesc desc))

parseOrigin :: Parser Origin
parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")
  where
    mkOrigin = Origin . S.pack

readOptionsParser :: Parser Component
readOptionsParser = Read <$> parseOrigin
                         <*> parseAddress
                         <*> parseStart
                         <*> parseEnd
  where
    parseAddress = argument (fmap fromString . str) (metavar "ADDRESS")
    parseStart = option $
        long "start"
        <> short 's'
        <> value 0
        <> showDefault
        <> help "Start time in nanoseconds since epoch"

    parseEnd = option $
        long "end"
        <> short 'e'
        <> value maxBound
        <> showDefault
        <> help "End time in nanoseconds since epoch"

listOptionsParser :: Parser Component
listOptionsParser = List <$> parseOrigin

--
-- Actual tools
--

runPrintDate :: IO ()
runPrintDate = do
    now <- getCurrentTime
    let time = formatTime defaultTimeLocale "%FT%TZ" now
    putStrLn time


runReadPoints :: String -> Origin -> Address -> Word64 -> Word64 -> IO ()
runReadPoints broker origin addr start end = do
    withReaderConnection broker $ \c ->
        runEffect $ for (readSimple addr start end origin c >-> decodeSimple)
                        (lift . print)

runListContents :: String -> Origin -> IO ()
runListContents broker origin = do
    withContentsConnection broker $ \c ->
        runEffect $ for (enumerateOrigin origin c) (lift . print)

--
-- Main program entry point
--

main :: IO ()
main = do
    Options{..} <- execParser helpfulParser

    let level = if debug
        then Debug
        else Quiet

    quit <- initializeProgram (package ++ "-" ++ version) level

    -- Run selected component.
    debugM "Main.main" "Running command"

    -- Although none of the components are running in the background, we get off
    -- of the main thread so that we can block the main thread on the quit
    -- semaphore, such that a user interrupt will kill the program.

    linkThread $ do
        case component of
            Time ->
                runPrintDate
            Read origin addr start end ->
                runReadPoints broker origin addr start end
            List origin ->
                runListContents broker origin
        putMVar quit ()

    takeMVar quit
    debugM "Main.main" "End"

