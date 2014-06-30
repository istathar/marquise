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

import Control.Exception
import Marquise.Classes
import Marquise.IO.Connection
import Vaultaire.Types
import Marquise.Types
import Data.ByteString(ByteString)

instance MarquiseWriterMonad IO where
    transmitBytes broker origin bytes =
        withConnection ("tcp://" ++ broker ++ ":5560") $ \c -> do
            result <- trySend origin bytes c
            case result of
                OnDisk -> return ()
                InvalidWriteOrigin -> throw InvalidOrigin

trySend :: Origin -> ByteString -> SocketState -> IO WriteResult
trySend origin bytes c = do
    send (PassThrough bytes) origin c
    recv c `catch` retryOnTimeout
  where
    retryOnTimeout :: MarquiseTimeout -> IO WriteResult
    retryOnTimeout _ = trySend origin bytes c
