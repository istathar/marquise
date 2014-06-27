{-# LANGUAGE RecordWildCards #-}

module Main where
import qualified Data.ByteString.Char8 as S
import Marquise.Client
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Log.Handler.Syslog
import System.Log.Logger
import Data.Monoid

data Options = Options
  { broker    :: String
  , debug     :: Bool
  , origin    :: Origin
  , namespace :: String }

helpfulParser :: Options -> O.ParserInfo Options
helpfulParser os = info (helper <*> optionsParser os) fullDesc

optionsParser :: Options -> O.Parser Options
optionsParser Options{..} = Options <$> parseBroker
                                    <*> parseDebug
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
        <> help "Set log level to DEBUG"

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")

    mkOrigin = Origin . S.pack

defaultOptions :: Options
defaultOptions = Options "localhost" False (Origin mempty) mempty

main :: IO ()
main = do
    Options{..} <- execParser . helpfulParser $ defaultOptions

    let log_level = if debug then DEBUG else WARNING
    logger <- openlog "vaultaire" [PID] USER log_level
    updateGlobalLogger rootLoggerName (addHandler logger . setLevel log_level)

    debugM "Main.main" "Logger initialized, starting marquise daemon"
    marquiseServer broker origin namespace
