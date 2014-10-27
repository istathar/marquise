
-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.Client (
    -- | * Utility functions
    -- Note: You may read MarquiseSpoolFileMonad m as IO.
      hashIdentifier
    , makeSpoolName
    , makeOrigin

    -- | * Contents daemon requests
    , withContentsConnection
    , withContentsConnectionT
    , requestUnique
    , makeSourceDict
    , updateSourceDict
    , removeSourceDict
    , enumerateOrigin
    , enumerateOriginResume

    -- | * Queuing data to be sent to vaultaire
    , createSpoolFiles
    , queueSimple
    , queueExtended
    , queueSourceDictUpdate
    , flush

    -- | Reading from Vaultaire
    , withReaderConnection
    , withReaderConnectionT
    , readExtended
    , readExtendedPoints
    , readExtendedPointsResume
    , readSimple
    , readSimplePoints
    , readSimplePointsResume
    , decodeExtended
    , decodeSimple

    -- * Types
    , SourceDict
    , SpoolName
    , SpoolFiles
    , Address(..)
    , Origin(..)
    , TimeStamp(..)
    , SimpleBurst(..)
    , SimplePoint(..)
    , SocketState(..)
    , Policy(..)

    -- * Marquise top-level
    , Marquise
    , MarquiseErrorType(..)
    , catchMarquiseP
    , withMarquiseHandler
    , crashOnMarquiseErrors
    , ignoreMarquiseErrors
    ) where

import           Control.Monad.Error hiding (forever)
import qualified Data.HashSet  as HS
import           Pipes
import qualified Pipes.Prelude as P

import           Marquise.Client.Core hiding (enumerateOrigin, readSimplePoints, readExtendedPoints)
import qualified Marquise.Client.Core as C
import           Marquise.IO.Connection
import           Marquise.IO ()
import           Marquise.Classes
import           Marquise.Types
import           Vaultaire.Types


-- | Retry policies
--   *NOTE* retries will re-use the same connection (e.g. socket) of the failed query.
--          to use a new connection, use the @*Resume@ functions directly.
--
data Policy = NoRetry | ForeverRetry | JustRetry Int

forever :: Monad m => conn -> Result a m conn -> Producer a m ()
forever conn act = _result act >>= maybe (return ()) (forever conn . ($conn))

retry :: Monad m => conn -> Int -> Result a m conn -> Producer a m ()
retry _    0 act = _result act >>  return ()
retry conn n act = _result act >>= maybe (return ()) (retry conn (n-1) . ($conn))

-- | Stream read every Address associated with the given Origin
enumerateOrigin
  :: MarquiseContentsMonad m conn
  => Policy
  -> Origin
  -> conn
  -> Producer (Address, SourceDict) (Marquise m) ()
enumerateOrigin pol org conn = case pol of
  NoRetry      ->                C.enumerateOrigin org conn
  ForeverRetry -> forever conn $   enumerateOriginResume org conn
  JustRetry n  -> retry conn n $   enumerateOriginResume org conn

-- | Stream read every SimplePoint from the Address between the given times
readSimplePoints
   :: MarquiseReaderMonad m conn
   => Policy
   -> Address
   -> TimeStamp
   -> TimeStamp
   -> Origin
   -> conn
   -> Producer SimplePoint (Marquise m) ()
readSimplePoints pol addr start end origin conn = case pol of
  NoRetry      ->                C.readSimplePoints       addr start end origin conn
  ForeverRetry -> forever conn $   readSimplePointsResume addr start end origin conn
  JustRetry n  -> retry conn n $   readSimplePointsResume addr start end origin conn

-- | Stream read every ExtendedPoint from the Address between the given times
readExtendedPoints
   :: MarquiseReaderMonad m conn
   => Policy
   -> Address
   -> TimeStamp
   -> TimeStamp
   -> Origin
   -> conn
   -> Producer ExtendedPoint (Marquise m) ()
readExtendedPoints pol addr start end origin conn = case pol of
  NoRetry      ->                C.readExtendedPoints       addr start end origin conn
  ForeverRetry -> forever conn $   readExtendedPointsResume addr start end origin conn
  JustRetry n  -> retry conn n $   readExtendedPointsResume addr start end origin conn

-- Resume ----------------------------------------------------------------------

-- | Like @enumerateOrigin@, but also returns a resumption pipe
--   for the user to run and get the rest of the addresses in a consistent manner
--   (i.e. any addresses already yielded will be ignored)
--
enumerateOriginResume :: MarquiseContentsMonad m conn
                      => Origin
                      -> conn
                      -> Result (Address, SourceDict) (Marquise m) conn
enumerateOriginResume origin conn = catchRecover
    (C.enumerateOrigin origin conn)
    (\x -> case x of
      Timeout (EnumOrigin seen)
        -> mkResumption $ \c -> Result $ _result (enumerateOriginResume origin c) >-> stop seen
      _ -> Result $ throwError x)
    where stop x = P.filter (not . flip HS.member x . fst)

-- | Like @readSimplePoints@, but also returns a resumption pipe that we can run and
--   get the rest of the result.
--
readSimplePointsResume
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Result SimplePoint (Marquise m) conn
readSimplePointsResume addr start end origin conn = catchRecover
    (C.readSimplePoints addr start end origin conn)
    (\x -> case x of
      -- to resume, read from the last point yielded (exclusive)
      Timeout (ReadPoints t)
        -> mkResumption $ \conn' -> ignoreFirst $ readSimplePointsResume addr t end origin conn'
      _ -> Result $ throwError x)

-- | Like @readExtendedPoints@, but also returns a resumption pipe that we can run and
--   get the rest of the result.
--
readExtendedPointsResume
    :: MarquiseReaderMonad m conn
    => Address
    -> TimeStamp
    -> TimeStamp
    -> Origin
    -> conn
    -> Result ExtendedPoint (Marquise m) conn
readExtendedPointsResume addr start end origin conn = catchRecover
    (C.readExtendedPoints addr start end origin conn)
    (\x -> case x of
      -- to resume, read from the last point yielded (exclusive)
      Timeout (ReadPoints t)
        -> mkResumption $ \conn' -> ignoreFirst $ readExtendedPointsResume addr t end origin conn'
      _ -> Result $ throwError x)
