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
    withConnection' ("tcp://" ++ broker ++ ":5560") $ \c -> do
      ack <- trySend origin bytes c
      case ack of
        OnDisk             -> return ()
        InvalidWriteOrigin -> throwError $ InvalidOrigin origin

-- | Tries to send some data.
--   Deals with @Timeout@ @MarquiseError@s by retrying indefinitely.
--
trySend :: Origin -> ByteString -> SocketState
        -> Marquise IO WriteResult -- ^ The errors in here cannot have any @Timeout@s
                                   --   it might be worth using extensible error sets to assert this, or not.
trySend origin bytes c = do
    send (PassThrough bytes) origin c
    retryOnTimeout $ recv c
  where retryOnTimeout :: Marquise IO WriteResult -> Marquise IO WriteResult
        retryOnTimeout act = act `catchError` (\Timeout -> trySend origin bytes c)
