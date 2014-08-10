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
  { broker    :: String
  , debug     :: Bool
  , quiet     :: Bool
  , origin    :: Origin
  , namespace :: String }

helpfulParser :: Options -> O.ParserInfo Options
helpfulParser os = info (helper <*> optionsParser os) fullDesc

optionsParser :: Options -> O.Parser Options
optionsParser Options{..} = Options <$> parseBroker
                                    <*> parseDebug
                                    <*> parseQuiet
                                    <*> parseOrigin
                                    <*> parseNameSpace
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

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")

    mkOrigin = Origin . S.pack

defaultOptions :: Options
defaultOptions = Options "localhost" False False (Origin mempty) mempty

main :: IO ()
main = do
    Options{..} <- execParser . helpfulParser $ defaultOptions

    let level = if debug
        then Debug
        else if quiet
            then Quiet
            else Normal

    quit <- initializeProgram (package ++ "-" ++ version) level

    a <- runMarquiseDaemon broker origin namespace quit

    -- wait forever
    wait a
    debugM "Main.main" "End"
