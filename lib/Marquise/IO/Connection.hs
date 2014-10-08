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
    withConnectionT,
    send,
    recv,
    SocketState(..),
) where

import           Control.Monad.Error
import qualified Control.Exception.Lifted as L
import           Data.List.NonEmpty (fromList)
import           System.ZMQ4 (Socket, Poll(..), Event(..), Dealer(..))
import qualified System.ZMQ4 as Z

import           Marquise.Types
import           Vaultaire.Types

-- | Wrapped ZMQ4 Socket + broker/IP
data SocketState = SocketState (Socket Dealer) String

-- | Performs operation f through broker.
--   This can be thought of as adding marquise errors to something to an error (effect) set.
withConnection :: String -> (SocketState -> IO a) -> Marquise IO a
withConnection broker f = catchSyncIO ZMQException $
    Z.withContext $ \ctx ->
    Z.withSocket ctx Dealer $ \s -> do
        Z.connect s broker
        f (SocketState s broker)

-- | Like @withConnection@, but for a continuation that already wraps @MarquiseError@s.
--   This can be thought of as adding connection-related errors to an error (effect) set.
withConnectionT :: String -> (SocketState -> Marquise IO a) -> Marquise IO a
withConnectionT broker f
  = L.bracket (catchSyncIO ZMQException $ do
                 c <- Z.context
                 s <- Z.socket c Dealer
                 Z.connect s broker
                 return (c, s))
              (\(ctx, sock) -> catchSyncIO ZMQException $ Z.close sock >> Z.term ctx)
              (\(_,   sock) -> f (SocketState sock broker))

send :: WireFormat request
     => request
     -> Origin
     -> SocketState
     -> Marquise IO ()
send request (Origin origin) (SocketState sock _)
  = catchSyncIO ZMQException
  $ Z.sendMulti sock (fromList [origin, toWire request])

recv :: WireFormat response
     => SocketState
     -> Marquise IO response
recv (SocketState sock endpoint) = do
  poll_result <- catchSyncIO ZMQException
               $ Z.poll timeout [Sock sock [In] Nothing]
  case poll_result of
    [[In]] -> do
      resp  <- catchSyncIO ZMQException
             $ Z.receiveMulti sock
      case resp of
          [msg] -> either (throwError . VaultaireException) return $ fromWire msg
          []    ->         throwError $ MalformedResponse "expected one message, received none"
          _     ->         throwError $ MalformedResponse "expected one message, received multiple"
    [[]] -> do
      -- Timeout, reconnect the socket so that we can be sure that a late
      -- response on the current connection isn't confused with a
      -- response to a later request.
      catchSyncIO ZMQException $ Z.disconnect sock endpoint
      catchSyncIO ZMQException $ Z.connect sock endpoint
      throwError Timeout
    _    -> throwError $ Other "Marquise.IO.Connection.recv: impossible"
  where timeout = 60000 -- milliseconds, 60s
