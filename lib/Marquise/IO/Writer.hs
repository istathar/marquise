--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.IO.Writer
(
) where

import Control.Monad.Error
import Data.ByteString (ByteString)

import Marquise.Classes
import Marquise.IO.Connection
import Marquise.Types
import Vaultaire.Types

instance MarquiseWriterMonad IO where
  transmitBytes broker origin bytes =
    withConnectionT ("tcp://" ++ broker ++ ":5560") $ \c -> do
      ack <- trySend origin bytes c
      case ack of
        OnDisk             -> return ()
        InvalidWriteOrigin -> throwError $ InvalidOrigin origin

-- | Tries to send some data.
--   Deals with @Timeout@ @MarquiseError@s by retrying indefinitely.
--
trySend :: Origin -> ByteString -> SocketState -> Marquise IO WriteResult
trySend origin bytes c = do
    send (PassThrough bytes) origin c
    recv c
