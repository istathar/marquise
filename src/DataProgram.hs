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
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Main where

import           Control.Concurrent.MVar
import           Control.Monad
import qualified Data.Attoparsec.Text as PT
import           Data.Binary.IEEE754
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.HashMap.Strict as HT
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word
import           Data.Csv (record)
import           Data.Aeson
import           Data.Aeson.TH (deriveJSON, defaultOptions)
import           Options.Applicative
import           Pipes
import qualified Pipes.Aeson.Unchecked as PA
import qualified Pipes.Csv as PC
import qualified Pipes.ByteString as PB
import           System.IO
import           System.Directory
import           System.FilePath
import           System.Locale
import           System.Log.Logger
import           Text.Printf

import           Marquise.Client
import           Package (package, version)
import           Vaultaire.Program
import           Vaultaire.Util
import           Vaultaire.Types (fromWire, toWire, sizeOfSourceCache)

--
-- Component line option parsing
--

data Options = Options
  { broker    :: String
  , outdir    :: FilePath
  , format    :: Format
  , debug     :: Bool
  , component :: Component }

data Format = JSON | CSV deriving Read

data Component
  = Now
  | Read    { origin  :: Origin
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
            , resume  :: Bool
            , start   :: TimeStamp
            , end     :: TimeStamp }
  | SourceCache { cacheFile :: FilePath }

type Tag = (Text, Text)

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parseBroker
                        <*> parseOutput
                        <*> parseOutputFormat
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
           long "output_dir"
        <> value "out"
        <> short 'o'
        <> help "Output directory"

    parseOutputFormat = option $
           long "output_format"
        <> short 'f'
        <> help "Supported: JSON or CSV"

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

    readCmd :: Parser Component
    readCmd = Read <$> parseOrigin
                   <*> parseAddress
                   <*> parseStart
                   <*> parseEnd

    listCmd :: Parser Component
    listCmd = List <$> parseOrigin

    fetchCmd = Fetch <$> parseOrigin
                     <*> parseResume
                     <*> parseStart
                     <*> parseEnd

    addCmd :: Parser Component
    addCmd = Add <$> parseOrigin <*> parseAddress <*> parseTags

    removeCmd :: Parser Component
    removeCmd = Remove <$> parseOrigin <*> parseAddress <*> parseTags

    sourceCacheCmd :: Parser Component
    sourceCacheCmd = SourceCache <$> parseFilePath
      where
        parseFilePath = argument str $ metavar "CACHEFILE"

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

    parseResume = switch $
        long "resume"
        <> short 'r'
        <> help "Fetch in resumption mode: ignoring all existing addresses in <output_dir>/addresses. To force re-fetching a specific address, remove it from this file. You might want to do this if you know the results for an address is incomplete."

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

instance ToJSON SimplePoint where
  toJSON (SimplePoint address timestamp payload) =
    object [ "address"   .= show address
           , "timestamp" .= formatTimestamp timestamp
           , "value"     .= formatValue payload ]

instance PC.ToRecord SimplePoint where
  toRecord (SimplePoint address timestamp payload) =
    record [ S.pack $ show address
           , S.pack $ formatTimestamp timestamp
           , S.pack $ formatValue payload ]

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

$(deriveJSON defaultOptions ''Address)
$(deriveJSON defaultOptions ''SourceDict)

encodePoints :: Monad m => Format -> Pipe SimplePoint ByteString m ()
encodePoints JSON = forever $ await >>= PA.encode >> yield "\n"
encodePoints CSV  = PC.encode

eval :: FilePath -> Format -> String -> Component -> IO ()

eval _ _ _ Now = do
  now <- getCurrentTime
  let time = formatTime defaultTimeLocale "%FT%TZ" now
  putStrLn time

eval out _ broker (List origin) =
  withFile (out ++ "/" ++ show origin) WriteMode $ \h ->
  withContentsConnection broker $ \conn ->
  runEffect $ enumerateOrigin origin conn
            >-> for cat PA.encode
            >-> PB.toHandle h

eval out format broker (Read origin addr start end) =
  withFile (out ++ "/" ++ show addr) WriteMode $ \h ->
  withReaderConnection broker $ \conn ->
  runEffect $   readSimple addr start end origin conn
            >-> decodeSimple
            >-> encodePoints format
            >-> PB.toHandle h

eval out format broker (Fetch origin resume start end) =
  withContentsConnection broker $ \mcontents ->
  withReaderConnection broker $ \mreader -> do
    exists  <- if resume
               then fmap (map (read :: String -> Address) . lines)
                  $ readFile (out ++ "/addresses")
               else return []
    runEffect $   enumerateOrigin origin mcontents
              >-> ignore exists
              >-> fetchAddress mreader
              >-> output
  where fetchAddress conn = forever $ do
          (addr, sd) <- await
          let d = concat [out, "/", show addr, "__", escape $ S.unpack $ toWire sd]
          liftIO $ createDirectoryIfMissing False d
          liftIO $ S.appendFile (d   ++ "/sd") (toWire sd)
          liftIO $   appendFile (out ++ "/addresses") (show addr ++ "\n")
          liftIO $ debugM "Main.main" ("Reading points from address " ++ show addr)
          for (   readSimple addr start end origin conn
              >-> decodeSimple
              >-> encodePoints format)
              $ yield . (d ++ "/points",)
        output = forever $ do
          (file, x) <- await
          -- If we need to avoid opening the output handle multiple times
          -- do so by keeping some state when yielding the points.
          liftIO $ S.appendFile file x
        ignore these = forever $ do
          (a, s) <- await
          if a `elem` these
          then liftIO $ debugM "Main.main" ("Ignoring address " ++ show a)
          else yield (a, s)
        escape = map (\c -> if isPathSeparator c then '_' else c)

eval _ _ broker (Add origin addr dict)
  = runDictOp updateSourceDict broker origin addr dict

eval _ _ broker (Remove origin addr dict)
  = runDictOp removeSourceDict broker origin addr dict

eval _ _ _ (SourceCache cacheFile) = do
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

    createDirectoryIfMissing False outdir

    quit <- initializeProgram (package ++ "-" ++ version) level

    -- Run selected component.
    debugM "Main.main" "Running command"

    -- Although none of the components are running in the background, we get off
    -- of the main thread so that we can block the main thread on the quit
    -- semaphore, such that a user interrupt will kill the program.

    linkThread $ do
      eval outdir format broker component
      putMVar quit ()

    takeMVar quit

    debugM "Main.main" "End"
