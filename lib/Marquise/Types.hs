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
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DeriveFunctor #-}

-- Our Base/BaseControl instances are simple enough to assert that
-- that they are decidable, monad-control needs this too.
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_HADDOCK hide, prune #-}

-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.Types
    ( -- * Data
      SpoolName(..)
    , SpoolFiles(..)
    , TimeStamp(..)
    , SimplePoint(..), ExtendedPoint(..)

      -- * Results
    , Result(..)
    , catchRecover
    , mkResumption
    , ignoreFirst

      -- * Errors
    , Marquise
    , unwrap, unMarquise, unMarquise'
    , MarquiseErrorType(..)
    , catchSyncIO, catchTryIO, catchMarquiseP
    , withMarquiseHandler
    , ErrorState(..)

      -- * Basic Logging
    , logInfo, logError
) where

import           Control.Applicative
import           Control.Monad.Base
import           Control.Monad.Error
import           Control.Monad.Morph
import           Control.Monad.Logger hiding (logInfo, logError)
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Either
import           Control.Monad.Trans.State.Strict
import           Control.Monad.State.Strict
import           Control.Error.Util
import           Control.Exception (IOException, SomeException)
import           Data.HashSet (HashSet)
import qualified Data.HashSet as HS
import qualified Data.Text    as T
import           Data.Either.Combinators
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import           Data.Word (Word64)
import           Pipes
import qualified Pipes.Lift as P
import qualified System.IO  as IO
import           System.Log.FastLogger

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
  deriving (Show, Eq)


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
  deriving (Show, Eq)


-- Result ----------------------------------------------------------------------

-- | A resumption producer that yields @a@ and might return a continuation that can be
--   run (with the same connection or a new one) to get the rest of the results.
newtype Result a m conn = Result { _result :: Producer a m (Maybe (conn -> Result a m conn)) }

catchRecover
  :: (Monad m)
  => Producer a (Marquise m) ()
  -> (MarquiseErrorType -> Result a (Marquise m) conn)
  -> Result a (Marquise m) conn
catchRecover p handler = Result $ catchMarquiseP (p >> return Nothing) (\e -> _result $ handler e)

mkResumption :: Monad m => (conn -> Result a m conn) -> Result a m conn
mkResumption act = Result $ return $ Just $ \conn2 -> act conn2

-- | Ignores the first streamed element.
ignoreFirst :: Monad m => Result a m c -> Result a m c
ignoreFirst (Result p) = Result $ do
  x <- lift $ next p
  case x of Left _          -> return Nothing
            Right (_, rest) -> rest

-- Errors ----------------------------------------------------------------------

-- | Handles everything that can fail so we don't ever just crash from an exception
--   but always provide opportunities to recover.
--
--   *NOTE*
--   This is a newtype because we want to define our own @MonadTransControl@ instance
--   that exposes the errors to be unwrapped and restored manually.
--   See @Marquise.Classes@
--
newtype Marquise m a = Marquise { marquise :: ErrorT MarquiseErrorType (StateT ErrorState m) a }
  deriving ( Functor, Applicative, Monad
           , MonadError MarquiseErrorType, MonadState ErrorState, MonadIO )

instance MonadTrans Marquise where
  lift = Marquise . lift . lift

instance MonadTransControl Marquise where
  data StT Marquise a = StMarquise { unStMarquise :: (Either MarquiseErrorType a, ErrorState) }
  liftWith f = Marquise $ ErrorT $ StateT $ \s ->
    liftM (, s)                                                            -- rewrap state
          (liftM return                                                    -- rewrap error
                 (f $ \t -> liftM StMarquise
                                  (runStateT (runErrorT $ marquise t) s))) -- unwrap error and state
  restoreT = Marquise . ErrorT . StateT . const . liftM unStMarquise
  {-# INLINE liftWith #-}
  {-# INLINE restoreT #-}

deriving instance MonadBase b m => MonadBase b (Marquise m)

instance MonadBaseControl b m => MonadBaseControl b (Marquise m) where
  newtype StM (Marquise m) a = StMMarquise { unStMMarquise :: ComposeSt Marquise m a}
  liftBaseWith = defaultLiftBaseWith StMMarquise
  restoreM     = defaultRestoreM   unStMMarquise
  {-# INLINE liftBaseWith #-}
  {-# INLINE restoreM #-}

-- | Rudimentary stdout logging
instance MonadIO m => MonadLogger (Marquise m) where
  monadLoggerLog _ _ _ msg = liftIO $ B8.hPutStrLn IO.stdout $ fromLogStr $ toLogStr msg

logInfo :: MonadLogger m => String -> m ()
logInfo = logInfoN . T.pack

logError :: MonadLogger m => String -> m ()
logError = logErrorN . T.pack

-- | Unwrap the insides of a @Marquise@ monad and keep them in the @StT@ from
--   @monad-control@, so we need to @restoreT@ manually.
unwrap :: Functor m => Marquise m a -> m (StT Marquise a)
unwrap = fmap StMarquise . flip runStateT None . runErrorT . marquise

unMarquise' :: Marquise m a -> m (Either MarquiseErrorType a, ErrorState)
unMarquise' = flip runStateT None . runErrorT . marquise

unMarquise :: Monad m => Marquise m a -> m (Either MarquiseErrorType a)
unMarquise = liftM fst . unMarquise'

-- | Information to recover from the failure of an operation.
--   it is up to the operation to decide what state it needs to recover.
--
data ErrorState
 = EnumOrigin { enumerated :: HashSet Address }
 | ReadPoints { latest :: TimeStamp }
 | None

instance Show ErrorState where
  show (EnumOrigin e) = concat ["failure state: got ", show (HS.size e)]
  show (ReadPoints t) = concat ["failure state: read last point ", show t]
  show None           = concat ["failure state: none"]

-- | All possible errors in a Marquise program.
--
data MarquiseErrorType
 = InvalidSpoolName   String
 | InvalidOrigin      Origin
 | Timeout            ErrorState                -- ^ timeout connecting to backend
 | MalformedResponse  String                    -- ^ unexected response from backend
 | VaultaireException SomeException             -- ^ handles all backend exceptions
 | ZMQException       SomeException             -- ^ handles all zmq exceptions
 | IOException        IOException               -- ^ handles all IO exceptions
 | Other              String                    -- ^ needed for the @Error@ instance until pipes move to @Except@

instance Show MarquiseErrorType where
  show (InvalidSpoolName s)     = "marquise: invalid spool name: "  ++ s
  show (InvalidOrigin x)        = "marquise: invalid origin: "      ++ (B8.unpack $ unOrigin x)
  show (Timeout x)              = "marquise: timeout at "           ++ show x
  show (MalformedResponse s)    = "marquise: unexpected response: " ++ s
  show (VaultaireException e)   = "marquise: vaultaire error: "     ++ show e
  show (ZMQException       e)   = "marquise: ZMQ error: "           ++ show e
  show (IOException        e)   = "marquise: IO error: "            ++ show e
  show (Other s)                = "marquise: error: "               ++ s

instance Error MarquiseErrorType where
  noMsg = Other "unknown"

-- | Catch a Marquise error inside a pipe
catchMarquiseP
  :: (Monad m)
  => Proxy a' a b' b (Marquise m) r
  -> (MarquiseErrorType -> Proxy a' a b' b (Marquise m) r)
  -> Proxy a' a b' b (Marquise m) r
catchMarquiseP act handler
  = hoist Marquise $ P.catchError (hoist marquise act) (hoist marquise . handler)

-- | Catch all synchorous IO exceptions and wrap them in @ErrorT@
catchSyncIO :: (SomeException -> MarquiseErrorType) -> IO a -> Marquise IO a
catchSyncIO f = Marquise . ErrorT . fmap (mapLeft f) . runEitherT . syncIO

-- | Catch only @IOException@s
catchTryIO  :: IO a -> Marquise IO a
catchTryIO = Marquise . ErrorT . fmap (mapLeft IOException) . runEitherT . tryIO

withMarquiseHandler :: Monad m => (MarquiseErrorType -> m a) -> Marquise m a -> m a
withMarquiseHandler f act = unMarquise act >>= either f return
