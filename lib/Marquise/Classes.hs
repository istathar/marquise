--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}

module Marquise.Classes
(
    MarquiseWriterMonad(..),
    MarquiseSpoolFileMonad(..),
    MarquiseReaderMonad(..),
    MarquiseContentsMonad(..),
) where

import Control.Monad.Error
import Data.ByteString (ByteString)
import qualified Control.Exception.Lifted as L
import qualified Data.ByteString.Lazy     as LB

import Marquise.Types
import Vaultaire.Types

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that the client and server will do.
class Monad m => MarquiseSpoolFileMonad m where
    randomSpoolFiles :: SpoolName -> Marquise m SpoolFiles

    createDirectories :: SpoolName -> Marquise m ()

    -- | Append to the spool file for points, i.e. data.
    --
    -- This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so. This assumes no state, so any file handles
    -- must be stashed globally or re-opened and closed.
    appendPoints :: SpoolFiles -> ByteString -> Marquise m ()

    -- | Append  to the spool file for contents updates, i.e. metadata.
    appendContents :: SpoolFiles -> ByteString -> Marquise m ()

    -- | Return an lazy bytestring and an IO action to signify that the burst
    -- has been completely sent.
    --
    -- May block until something is actually spooled up.
    nextPoints :: SpoolName -> Marquise m (Maybe (LB.ByteString, m ()))
    nextContents :: SpoolName -> Marquise m (Maybe (LB.ByteString, m ()))

    -- | Close any open handles and flush all previously appended datum to disk
    close :: SpoolFiles -> Marquise m ()

-- | Monad encapsulating writer operations. Note there is an instance for IO
-- in IO/Writer.hs
class Monad m => MarquiseWriterMonad m where
    -- | Send bytes upstream.
     --  returns: - result when an ACK is received.
     --           - error when an exception happens.
    transmitBytes :: String      -- ^ Broker address
                  -> Origin      -- ^ Origin
                  -> ByteString  -- ^ Bytes to send
                  -> Marquise m ()

-- | Monad encapsulating reader operations. Note there is an instance for
-- IO SocketState in IO/Contents.hs
class Monad m => MarquiseContentsMonad m connection | m -> connection where
    withContentsConnection :: String -> (connection -> m a) -> Marquise m a
    sendContentsRequest    :: ContentsOperation -> Origin -> connection -> Marquise m ()
    recvContentsResponse   :: connection -> Marquise m ContentsResponse

-- | Monad encapsulating reader operations. Note there is an instance for
-- IO SocketState in IO/Reader.hs
class Monad m => MarquiseReaderMonad m connection | m -> connection where
    withReaderConnection :: String -> (connection -> m a) -> Marquise m a
    sendReaderRequest    :: ReadRequest -> Origin -> connection -> Marquise m ()
    recvReaderResponse   :: connection -> Marquise m ReadStream
