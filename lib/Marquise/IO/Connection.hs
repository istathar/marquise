--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Connection
(
    withConnection,
    send,
    recv,
    SocketState(..),
) where

import Control.Exception
import Data.List.NonEmpty (fromList)
import Marquise.Types
import System.ZMQ4 hiding (send)
import Vaultaire.Types

data SocketState = SocketState (Socket Dealer) String

withConnection :: String -> (SocketState -> IO a) -> IO a
withConnection broker f =
    withContext $ \ctx ->
    withSocket ctx Dealer $ \s -> do
        connect s broker
        f (SocketState s broker)

send :: WireFormat request
     => request
     -> Origin
     -> SocketState
     -> IO ()
send request (Origin origin) (SocketState sock _) =
    sendMulti sock (fromList [origin, toWire request])

recv :: WireFormat response
     => SocketState
     -> IO response
recv (SocketState sock endpoint) = do
    poll_result <- poll timeout [Sock sock [In] Nothing]
    case poll_result of
        [[In]] -> do
            resp <- receiveMulti sock
            return $ case resp of
                [msg] -> either throw id $ fromWire msg
                _ -> throw $ userError "expected one msg"
        [[]] -> do
            -- Timeout, reconnect the socket so that we can be sure that a late
            -- response on the current connection isn't confused with a
            -- response to a later request.
            disconnect sock endpoint
            connect sock endpoint
            throw MarquiseTimeout
        _ -> error "Marquise.IO.Connection.recv: impossible"
  where
    timeout = 60000 -- milliseconds, 60s
