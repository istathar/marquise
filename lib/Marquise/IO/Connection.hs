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
    withConnectionT,
    send,
    recv,
    SocketState(..),
) where

import           Control.Monad.Error
import           Control.Monad.Trans.Control
import           Control.Monad.State
import           Data.List.NonEmpty (fromList)
import           System.ZMQ4 (Socket, Poll(..), Event(..), Dealer(..))
import qualified System.ZMQ4 as Z

import           Marquise.Types
import           Vaultaire.Types

-- | Wrapped ZMQ4 Socket + broker/IP
data SocketState = SocketState (Socket Dealer) String

-- | Performs operation f through broker.
withConnection :: String -> (SocketState -> IO a) -> IO a
withConnection broker f =
    Z.withContext $ \ctx ->
    Z.withSocket ctx Dealer $ \s -> do
        Z.connect s broker
        f (SocketState s broker)

withConnectionT :: String -> (SocketState -> Marquise IO a) -> Marquise IO a
withConnectionT broker act = restoreT $ withConnection broker (unwrap . act)

send :: WireFormat request
     => request
     -> Origin
     -> SocketState
     -> Marquise IO ()
send request (Origin origin) (SocketState sock _)
  = do catchSyncIO ZMQException $ Z.sendMulti sock (fromList [origin, toWire request])

recv :: WireFormat response
     => SocketState
     -> Marquise IO response
recv (SocketState sock endpoint) = do
  recover <- get
  poll_result <- catchSyncIO ZMQException $ Z.poll timeout [Sock sock [In] Nothing]
  case poll_result of
    [[In]] -> do
      resp  <- catchSyncIO ZMQException $ Z.receiveMulti sock
      case resp of
          [msg] -> either (throwError . VaultaireException) return $ fromWire msg
          []    ->         throwError $ MalformedResponse "expected one message, received none"
          _     ->         throwError $ MalformedResponse "expected one message, received multiple"
    [[]] -> do
      -- Timeout, reconnect the socket so that we can be sure that a late
      -- response on the current connection isn't confused with a
      -- response to a later request.
      catchSyncIO ZMQException $ do
        Z.disconnect sock endpoint
        Z.connect sock endpoint
      throwError $ Timeout recover
    _    -> throwError $ Other "Marquise.IO.Connection.recv: impossible"
  where timeout = 60000 -- milliseconds, 60s
