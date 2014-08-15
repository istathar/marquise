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
import Control.Exception
import qualified Data.Binary as B
import qualified Data.Binary.Get as G
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Data.IORef
import Data.Monoid
import qualified Data.Set as Set
import Data.Word
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.IO
import System.Log.Logger

import Marquise.Client
import Marquise.Server
import Package (package, version)
import Vaultaire.Program

data Options = Options
  { broker    :: String
  , debug     :: Bool
  , quiet     :: Bool
  , origin    :: Origin
  , namespace :: String
  , cacheFile :: String }

helpfulParser :: Options -> O.ParserInfo Options
helpfulParser os = info (helper <*> optionsParser os) fullDesc

optionsParser :: Options -> O.Parser Options
optionsParser Options{..} = Options <$> parseBroker
                                    <*> parseDebug
                                    <*> parseQuiet
                                    <*> parseOrigin
                                    <*> parseNameSpace
                                    <*> parseCacheFile
  where
    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value broker
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
        <> value defaultCacheLoc        
        <> help "Location to read/write cached SourceDicts"

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")

    mkOrigin = Origin . S.pack

defaultOptions :: Options
defaultOptions = Options "localhost" False False (Origin mempty) mempty defaultCacheLoc

defaultCacheLoc :: String
defaultCacheLoc = "/var/tmp/source_dict_hash_cache"

decodeCache :: L.ByteString -> Either String (Set.Set Word64)
decodeCache rawData =
    let result = G.runGetOrFail B.get rawData in
    case result of
        Left (_, _, e)         -> Left e
        Right (_, _, assocList) -> Right (Set.fromList assocList)

encodeCache :: Set.Set Word64 -> L.ByteString
encodeCache cache = B.encode $ Set.toList cache

main :: IO ()
main = do
    Options{..} <- execParser . helpfulParser $ defaultOptions

    let level = if debug
        then Debug
        else if quiet
            then Quiet
            else Normal

    quit <- initializeProgram (package ++ "-" ++ version) level
    
    initCache <- do
        let setup = openFile cacheFile ReadWriteMode
        let teardown = hClose
        bracket setup teardown $ (\h -> do
            contents <- L.hGetContents h
            let result = decodeCache contents
            case result of
                Left e -> do
                    warningM "Main.initCache" $ concat ["Error decoding hash file: ", show e, " Continuing with empty initial cache"]      
                    return Set.empty
                Right cache -> return cache)
    cacheRef <- newIORef initCache
    a <- runMarquiseDaemon broker origin namespace quit cacheRef

    -- wait forever
    wait a
    finalCache <- readIORef cacheRef
    let encodedCache  = encodeCache finalCache
    bracket (openFile cacheFile WriteMode) (hClose) (\h -> L.hPut h encodedCache)
    debugM "Main.main" "End"
