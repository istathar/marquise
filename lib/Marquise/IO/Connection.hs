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
-- Hide warnings for the deprecated ErrorT transformer:
{-# OPTIONS_GHC -fno-warn-warnings-deprecations #-}

module Marquise.IO.Connection
(   withConnection,
    send,
    recv,
    SocketState(..),
) where

import Data.List.NonEmpty (fromList)
import System.ZMQ4 (Dealer (..), Event (..), Poll (..), Socket)
import qualified System.ZMQ4 as Z
import qualified Control.Exception as E

import Marquise.Types
import Vaultaire.Types

-- | Wrapped ZMQ4 Socket + broker/IP
data SocketState = SocketState (Socket Dealer) String

-- | Performs operation f through broker.
withConnection :: String -> (SocketState -> IO a) -> IO a
withConnection broker f =
    Z.withContext $ \ctx ->
    Z.withSocket ctx Dealer $ \s -> do
        Z.connect s broker
        f (SocketState s broker)

send :: WireFormat request
     => request
     -> Origin
     -> SocketState
     -> IO ()
send request (Origin origin) (SocketState sock _)
  = Z.sendMulti sock (fromList [origin, toWire request])

recv :: WireFormat response
     => SocketState
     -> IO response
recv (SocketState sock endpoint) = do
  poll_result <- Z.poll timeout [Sock sock [In] Nothing]
  case poll_result of
    [[In]] -> do
      resp  <- Z.receiveMulti sock
      case resp of
          [msg] -> either E.throw return $ fromWire msg
          []    -> E.throw $ MarquiseException "expected one message, received none"
          _     -> E.throw $ MarquiseException "expected one message, received multiple"
    [[]] -> do
      -- Timeout, reconnect the socket so that we can be sure that a late
      -- response on the current connection isn't confused with a
      -- response to a later request.
      Z.disconnect sock endpoint
      Z.connect sock endpoint
      E.throw $ MarquiseException "timeout"
    _    -> E.throw $ MarquiseException "Marquise.IO.Connection.recv: impossible"
  where timeout = 30 * 60 * 1000 -- milliseconds, 30m
