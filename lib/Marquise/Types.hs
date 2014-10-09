--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_HADDOCK hide, prune #-}
-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.Types
(
    SpoolName(..),
    SpoolFiles(..),
    TimeStamp(..),
    SimplePoint(..),
    ExtendedPoint(..),
    Marquise(..), MarquiseError(..), unwrap, catchSyncIO, catchTryIO
) where

import           Control.Applicative
import           Control.Monad.Error
import           Control.Monad.Morph
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Either
import           Control.Error.Util
import           Control.Exception (IOException, SomeException)
import           Data.Either.Combinators
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import           Data.Word (Word64)

import           Vaultaire.Types

-- | A NameSpace implies a certain amount of Marquise server-side state. This
-- state being the Marquise server's authentication and origin configuration.
newtype SpoolName = SpoolName { unSpoolName :: String }
  deriving (Eq, Show)

-- | SpoolFiles simple wraps around two file paths.
-- One for queuing points updates, one for queuing contents updates
data SpoolFiles = SpoolFiles { pointsSpoolFile   :: FilePath
                             , contentsSpoolFile :: FilePath }
  deriving (Eq, Show)

-- | SimplePoints are simply wrapped packets for Vaultaire
-- Each consists of 24 bytes:
-- An 8 byte Address
-- An 8 byte Timestamp (nanoseconds since Unix epoch)
-- An 8 byte Payload
data SimplePoint = SimplePoint { simpleAddress :: Address
                               , simpleTime    :: TimeStamp
                               , simplePayload :: Word64 }
  deriving Show


-- | ExtendedPoints are simply wrapped packets for Vaultaire
-- Each consists of 16 + 'length' bytes:
-- An 8 byte Address
-- An 8 byte Time (in nanoseconds since Unix epoch)
-- A 'length' byte Payload
-- On the wire their equivalent representation takes up
-- 24 + 'length' bytes with format:
-- 8 byte Address, 8 byte Time, 8 byte Length, Payload
data ExtendedPoint = ExtendedPoint { extendedAddress :: Address
                                   , extendedTime    :: TimeStamp
                                   , extendedPayload :: ByteString }
  deriving Show

-- Errors ----------------------------------------------------------------------

-- | Handles everything that can fail so we don't ever just crash from an exception
--   but always provide opportunities to recover.
--
--   *NOTE* This is a newtype because we want to define our own @MonadTransControl@ instance
--   that exposes the monad state constructor to be unwrapped and restored manually.
--   See Marquise.Classes
--
newtype Marquise m a = Marquise { marquise :: ErrorT MarquiseError m a }
  deriving ( Functor, Applicative, Monad, MonadTrans
           , MFunctor, MMonad )

instance MonadTransControl Marquise where
  newtype StT Marquise a = StMarquise { unStError :: Either MarquiseError a }
  liftWith f = Marquise $ ErrorT $ liftM return $ f $ liftM StMarquise . runErrorT . marquise
  restoreT   = Marquise . ErrorT . liftM unStError

unwrap :: Functor m => Marquise m a -> m (StT Marquise a)
unwrap =  fmap StMarquise . runErrorT . marquise

data MarquiseError
 = InvalidSpoolName   String
 | InvalidOrigin      Origin
 | Timeout                           -- ^ timeout connecting to backend
 | MalformedResponse  String         -- ^ unexected response from backend
 | VaultaireException SomeException  -- ^ handles all backend exceptions
 | ZMQException       SomeException  -- ^ handles all zmq exceptions
 | IOException        IOException    -- ^ handles all IO exceptions
 | Other              String         -- ^ needed for the @Error@ instance until pipes move to @Except@

instance Show MarquiseError where
  show (InvalidSpoolName s)   = "marquise: invalid spool name: "  ++ s
  show (InvalidOrigin x)      = "marquise: invalid origin: "      ++ (B8.unpack $ unOrigin x)
  show  Timeout               = "marquise: timeout"
  show (MalformedResponse s)  = "marquise: unexpected response: " ++ s
  show (VaultaireException e) = "marquise: vaultaire error: "     ++ show e
  show (ZMQException       e) = "marquise: ZMQ error: "           ++ show e
  show (IOException        e) = "marquise: IO error: "            ++ show e
  show (Other s)              = "marquise: error: "               ++ s

instance Error MarquiseError where
  noMsg = Other "unknown"

-- | Catch all synchorous IO exceptions and wrap them in @ErrorT@
catchSyncIO :: (SomeException -> MarquiseError) -> IO a -> Marquise IO a
catchSyncIO f = Marquise . ErrorT . fmap (mapLeft f) . runEitherT . syncIO

catchTryIO  :: IO a -> Marquise IO a
catchTryIO = Marquise . ErrorT . fmap (mapLeft IOException) . runEitherT . tryIO
