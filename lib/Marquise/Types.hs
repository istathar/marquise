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
{-# OPTIONS_HADDOCK hide, prune #-}

module Marquise.Types
(
    SpoolName(..),
    SpoolFiles(..),
    TimeStamp(..),
    SimplePoint(..),
    ExtendedPoint(..),
    InvalidSpoolName(..),
    InvalidOrigin(..),
    MarquiseTimeout(..),
) where

import Control.Exception
import Data.ByteString (ByteString)
import Data.Typeable
import Data.Word (Word64)
import Vaultaire.Types

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

-- | Empty data constructs/types for exception handling

data InvalidSpoolName = InvalidSpoolName
  deriving (Show, Typeable)

instance Exception InvalidSpoolName

data InvalidOrigin = InvalidOrigin
  deriving (Show, Typeable)

instance Exception InvalidOrigin

data MarquiseTimeout = MarquiseTimeout
  deriving (Show, Typeable)

instance Exception MarquiseTimeout
