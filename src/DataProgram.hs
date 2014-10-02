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
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MonadComprehensions #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import           Control.Concurrent.MVar
import qualified Data.Attoparsec.Text as PT
import           Data.Binary.IEEE754
import qualified Data.ByteString.Char8 as S
import qualified Data.HashMap.Strict as HT
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word
import           Options.Applicative
import           Pipes
import qualified Pipes.Prelude as P
import           System.IO
import           System.Locale
import           System.Log.Logger
import           Text.Printf

import           Marquise.Client
import           Package (package, version)
import           Vaultaire.Program
import           Vaultaire.Util
import           Vaultaire.Types (fromWire, sizeOfSourceCache)

--
-- Component line option parsing
--

data Options = Options
  { broker    :: String
  , output    :: FilePath
  , debug     :: Bool
  , component :: Component }

data Component
  = Now
  | Read    { raw     :: Bool
            , origin  :: Origin
            , address :: Address
            , start   :: TimeStamp
            , end     :: TimeStamp }
  | List    { origin  :: Origin }
  | Add     { origin  :: Origin
            , addr    :: Address
            , dict    :: [Tag] }
  | Remove  { origin  :: Origin
            , addr    :: Address
            , dict    :: [Tag] }
  | Fetch   { origin  :: Origin
            , start   :: TimeStamp
            , end     :: TimeStamp
            , raw     :: Bool }
  | SourceCache { cacheFile :: FilePath }

type Tag = (Text, Text)

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parseBroker
                        <*> parseOutput
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

    parseOutput = strOption $
           long "output_file"
        <> value "data.out"
        <> short 'o'

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Output lots of debugging information"

    parseComponents = subparser
       (   parseTimeComponent
        <> parseReadComponent
        <> parseListComponent
        <> parseFetchComponent
        <> parseAddComponent
        <> parseRemoveComponent
        <> parseSourceCacheComponent )

    parseTimeComponent =
        componentHelper "now" (pure Now) "Display the current time"

    parseReadComponent =
        componentHelper "read" readCmd "Read points from a given address and time range"

    parseListComponent =
        componentHelper "list" listCmd "List addresses and metadata in origin"

    parseFetchComponent =
        componentHelper "fetch" fetchCmd "Fetch all data from origin"

    parseAddComponent =
        componentHelper "add-source" addCmd "Add some tags to an address"

    parseRemoveComponent =
        componentHelper "remove-source" removeCmd "Remove some tags from an address, does nothing if the tags do not exist"

    parseSourceCacheComponent =
        componentHelper "source-cache" sourceCacheCmd "Validate the contents of a Marquise daemon source cache file"

    componentHelper cmd_name parser desc =
        command cmd_name (info (helper <*> parser) (progDesc desc))

    parseAddress :: Parser Address
    parseAddress = argument (fmap fromString . str) (metavar "ADDRESS")

    parseOrigin :: Parser Origin
    parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")
      where
        mkOrigin = Origin . S.pack

    parseTags :: Parser [Tag]
    parseTags = many $ argument (fmap mkTag . str) (metavar "TAG")
      where
        mkTag x = case PT.parseOnly tag $ T.pack x of
          Left _  -> error "data: invalid tag format"
          Right y -> y
        tag = (,) <$> PT.takeWhile (/= ':')
                  <* ":"
                  <*> PT.takeWhile (/= ',')

    readCmd :: Parser Component
    readCmd = Read <$> parseRaw
                   <*> parseOrigin
                   <*> parseAddress
                   <*> parseStart
                   <*> parseEnd

    listCmd :: Parser Component
    listCmd = List <$> parseOrigin

    fetchCmd = Fetch <$> parseOrigin
                     <*> parseStart
                     <*> parseEnd
                     <*> parseRaw

    addCmd :: Parser Component
    addCmd = Add <$> parseOrigin <*> parseAddress <*> parseTags

    removeCmd :: Parser Component
    removeCmd = Remove <$> parseOrigin <*> parseAddress <*> parseTags

    sourceCacheCmd :: Parser Component
    sourceCacheCmd = SourceCache <$> parseFilePath
      where
        parseFilePath = argument str $ metavar "CACHEFILE"

    parseRaw = switch $
        long "raw"
        <> short 'r'
        <> help "Output values in a raw form (human-readable otherwise)"

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

--
-- Actual tools
--

displayPoint :: Bool -> SimplePoint -> String
displayPoint raw (SimplePoint address timestamp payload) =
    if raw
        then
            show address ++ "," ++ show timestamp ++ "," ++ show payload
        else
            show address ++ "  " ++ formatTimestamp timestamp ++ " " ++ formatValue payload
  where
    formatTimestamp :: TimeStamp -> String
    formatTimestamp (TimeStamp t) =
      let
        seconds = posixSecondsToUTCTime $ realToFrac $ (fromIntegral t / 1000000000 :: Rational)
        iso8601 = formatTime defaultTimeLocale "%FT%T.%q" seconds
      in
        (take 29 iso8601) ++ "Z"

{-
    Take a stab at differentiating between raw integers and encoded floats.
    The 2^51 is somewhat arbitrary, being one less bit than the size of the
    significand in an IEEE 754 64-bit double. Seems safe to assume anything
    larger than that was in fact an encoded float; 2^32 (aka 10^9) is too small
    — we have counters bigger than that — but nothing has gone past 10^15 so
    there seemed plenty of headroom. At the end of the day this is just a
    convenience; if you need the real value and know its interpretation then
    you can request raw (in this program) output or actual bytes (via reader
    daemon).
-}
    formatValue :: Word64 -> String
    formatValue v = if v > (2^(51 :: Int) :: Word64)
        then
            printf "% 24.6f" (wordToDouble v)
        else
            printf "% 17d" v

eval :: String -> Component -> IO ()

eval _ Now = do
  now <- getCurrentTime
  let time = formatTime defaultTimeLocale "%FT%TZ" now
  putStrLn time

eval broker (List origin)
  = runQuery broker cat $ \conn ->
    enumerateOrigin origin conn

eval broker (Read raw origin addr start end)
  = runQuery broker (P.map $ displayPoint raw) $ \conn ->
    readSimple addr start end origin conn >-> decodeSimple

eval broker (Fetch origin start end raw)
  = runQuery broker (P.map $ displayPoint raw) $ \conn -> enumerate
  [ point
  | (addr, _) <- Select $   enumerateOrigin origin conn
  , point     <- Select $   readSimple addr start end origin conn
                        >-> decodeSimple ]

eval broker (Add origin addr dict)
  = runDictOp updateSourceDict broker origin addr dict

eval broker (Remove origin addr dict)
  = runDictOp removeSourceDict broker origin addr dict

eval _ (SourceCache cacheFile) = do
  bits <- withFile cacheFile ReadMode S.hGetContents
  case fromWire bits of
      Left e -> putStrLn . concat $
          [ "Error parsing cache file: "
          , show e
          ]
      Right cache -> putStrLn . concat $
          [ "Valid Marquise source cache. Contains "
          , show . sizeOfSourceCache $ cache
          , " entries."
          ]

runQuery broker pretty query
  = withContentsConnection broker
  $ \conn -> runEffect
  $ query conn >-> pretty >-> P.print

runDictOp op broker origin addr ts = do
  let dict = case makeSourceDict $ HT.fromList ts of
                  Left e  -> error e
                  Right a -> a
  withContentsConnection broker $ \c ->
        op addr dict origin c


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

    --linkThread $ do
    eval broker component
    --  putMVar quit ()

    debugM "Main.main" "End"
