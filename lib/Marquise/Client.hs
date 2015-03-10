{-# LANGUAGE MultiParamTypeClasses #-}

-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.Client (
    -- | * Utility functions
    -- Note: You may read MarquiseSpoolFileMonad m as IO.
      hashIdentifier
    , makeSpoolName
    , makeOrigin
    , newRandomPointsSpoolFile
    , newRandomContentsSpoolFile

    -- | * Contents daemon requests
    , withContentsConnection
    , requestUnique
    , makeSourceDict
    , updateSourceDict
    , removeSourceDict
    , enumerateOrigin

    -- | * Queuing data to be sent to vaultaire
    , createSpoolFiles
    , queueSimple
    , queueExtended
    , queueSourceDictUpdate
    , flush

    -- | Reading from Vaultaire
    , withReaderConnection
    , readExtended
    , readExtendedPoints
    , readSimple
    , readSimplePoints
    , decodeExtended
    , decodeSimple

    -- * Types
    , SourceDict
    , SpoolName
    , SpoolFiles
    , Address(..)
    , Origin(..)
    , TimeStamp(..)
    , ExtendedBurst(..)
    , ExtendedPoint(..)
    , SimpleBurst(..)
    , SimplePoint(..)
    , SocketState(..)
    ) where

import Marquise.Classes
import Marquise.Client.Core 
import Marquise.IO ()
import Marquise.IO.Connection
import Marquise.IO.SpoolFile
import Marquise.Types
import Vaultaire.Types
