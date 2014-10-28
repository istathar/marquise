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

      -- * Marquise monad
    , Marquise
    , MarquiseErrorType(..)
    , catchMarquise
    , catchMarquiseAll
    , withMarquiseHandler
    , crashOnMarquiseErrors
    , ignoreMarquiseErrors

      -- * Errors recovery
    , ErrorState(..)
    , unwrap
    , catchSyncIO, catchSyncIO_
    , catchTryIO, catchTryIO_
    , recoverable

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
catchRecover p handler = Result $ catchMarquise (p >> return Nothing) (\e -> _result $ handler e)

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
  show (EnumOrigin e) = concat ["fail at: got ", show (HS.size e)]
  show (ReadPoints t) = concat ["fail at: read last point ", show t]
  show None           = concat ["fail at: none"]

-- | All possible errors in a Marquise program.
--
data MarquiseErrorType
 = InvalidSpoolName   String
 | InvalidOrigin      Origin
 | Timeout            ErrorState                -- ^ timeout connecting to backend
 | MalformedResponse  ErrorState String         -- ^ unexected response from backend
 | VaultaireException ErrorState SomeException  -- ^ handles all backend exceptions
 | ZMQException       ErrorState SomeException  -- ^ handles all zmq exceptions
 | IOException        ErrorState IOException    -- ^ handles all IO exceptions
 | Other              String                    -- ^ needed for the @Error@ instance until pipes move to @Except@

instance Show MarquiseErrorType where
  show (InvalidSpoolName s)     = concat ["marquise: invalid spool name: ", s]
  show (InvalidOrigin x)        = concat ["marquise: invalid origin: ", B8.unpack $ unOrigin x]
  show (Timeout x)              = concat ["marquise: timeout\n", show x]
  show (MalformedResponse s a)  = concat ["marquise: unexpected response: ", a, "\n", show s]
  show (VaultaireException s e) = concat ["marquise: vaultaire error: ", show e, "\n", show s]
  show (ZMQException s e)       = concat ["marquise: ZMQ error: ", show e, "\n", show s]
  show (IOException s e)        = concat ["marquise: IO error: ", show e, "\n", show s]
  show (Other s)                = concat ["marquise: error: ", s]

instance Error MarquiseErrorType where
  noMsg = Other "unknown"

-- | Errors with an @ErrorState@ inside, which can be used to recover.
recoverable :: MarquiseErrorType -> Bool
recoverable (Timeout _)              = True
recoverable (MalformedResponse _ _)  = True
recoverable (VaultaireException _ _) = True
recoverable (ZMQException _ _)       = True
recoverable (IOException _ _)        = True
recoverable _                        = False

-- Handle errors insde the pipe -------------------------------------------------

-- | Handle some Marquise errors inside a pipe
catchMarquise
  :: (Monad m)
  => Proxy a' a b' b (Marquise m) r
  -> (MarquiseErrorType -> Proxy a' a b' b (Marquise m) r)
  -> Proxy a' a b' b (Marquise m) r
catchMarquise act handler
  = hoist Marquise
  $ P.catchError (hoist marquise act) (hoist marquise . handler)

-- | Handle all Marquise errors inside a pipe.
--   The user is responsible to handling every error case.
--   If there are any unhandled error cases, the pipe will fail and return the error.
catchMarquiseAll
  :: (Monad m)
  => Proxy a' a b' b (Marquise m) r
  -> (MarquiseErrorType -> Proxy a' a b' b (Marquise m) r)
  -> Proxy a' a b' b m (Either MarquiseErrorType r)
catchMarquiseAll act handler
  = P.evalStateP None $ P.runErrorP
  $ P.catchError (hoist marquise act) (hoist marquise . handler)

-- Handle errors outside the pipe -----------------------------------------------

-- | Supply an error handler to capture all errors from the Marquise monad.
withMarquiseHandler :: Monad m => (MarquiseErrorType -> m a) -> Marquise m a -> m a
withMarquiseHandler f act = unMarquise act >>= either f return

-- | Crash on all Marquise errors.
crashOnMarquiseErrors :: Monad m => Marquise m a -> m a
crashOnMarquiseErrors = withMarquiseHandler (error . show)

-- | Ignore Marquise errors, for computations that do not have a return value.
ignoreMarquiseErrors :: Monad m => Marquise m () -> m ()
ignoreMarquiseErrors = withMarquiseHandler (const $ return ())

-- Helpers ---------------------------------------------------------------------

-- | Catch all synchorous exceptions and wrap them in the Marquise monad
--   (sync exceptions include IO exceptions)
catchSyncIO :: (SomeException -> MarquiseErrorType) -> IO a -> Marquise IO a
catchSyncIO constructor = Marquise . ErrorT . fmap (mapLeft constructor) . runEitherT . syncIO

-- | Catch all synchorous exceptions, without an error state handler
catchSyncIO_ :: (ErrorState -> SomeException -> MarquiseErrorType) -> IO a -> Marquise IO a
catchSyncIO_ constructor = Marquise . ErrorT . fmap (mapLeft (constructor None)) . runEitherT . syncIO

-- | Catch all IO exceptions and wrap them in the Marquise monad, with a state from which to recover
catchTryIO :: ErrorState -> IO a -> Marquise IO a
catchTryIO s = Marquise . ErrorT . fmap (mapLeft (IOException s)) . runEitherT . tryIO

-- | Catch all IO exceptions, without a state from which to recover
catchTryIO_ :: IO a -> Marquise IO a
catchTryIO_ = catchTryIO None
