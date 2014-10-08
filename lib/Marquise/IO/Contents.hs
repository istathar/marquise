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
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Contents
(
) where

import Marquise.Classes
import Marquise.Types
import Marquise.IO.Connection

instance MarquiseContentsMonad IO SocketState where
    withContentsConnection = withConnection . endpoint
    sendContentsRequest = send
    recvContentsResponse = recv

withContentsConnectionT :: String -> (SocketState -> Marquise IO a) -> Marquise IO a
withContentsConnectionT = withConnectionT . endpoint

endpoint broker = "tcp://" ++ broker ++ ":5580"
