{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}

module Store where

import Control.Applicative
import Control.Monad.State.Strict
import Control.Monad.Error
import Control.Lens hiding (act, op)
import Data.Map (Map)
import qualified Data.Map as M

import Vaultaire.Types
import Marquise.Types
import Marquise.Classes

data PureStore
   = PureStore { _contents      :: [Data (Address, SourceDict)]
               , _points        :: [Data (SimpleBurst, SimplePoint)]
               -- No multiplexing, this seems to be the desired behaviour
               -- for each ZMQ connection.
               , _contentsConns :: Map Name ContentsConn
               , _pointsConns   :: Map Name PointsConn }

data Data a = Data a | FakeError

-- current operation and last sent response
type ContentsConn = (ContentsOperation, Maybe ContentsResponse)
type PointsConn   = (ReadRequest,       Maybe ReadStream)

newtype Name = Name String
        deriving (Eq, Ord)
newtype Store a = Store { store :: State PureStore a }
        deriving (Functor, Applicative, Monad, MonadState PureStore)

makeLenses ''PureStore
makeLenses ''Store

instance MarquiseContentsMonad Store Name where
  sendContentsRequest op _ i = lift $ contentsConns %= M.insert i (op, Nothing)
  recvContentsResponse i = do
    conn <- lift $ use contentsConns >>= return . M.lookup i
    maybe (throwError $ Other "no connection")
          (\c -> case fst c of ContentsListRequest -> lift (use contents) >>= next c)
          conn
    where next _ [] = return EndOfContentsList
          next _ (FakeError:_) = get >>= throwError . Timeout
          next c (Data (a,d):xs) = let resp = ContentsListEntry a d
                                      in  if Just resp == snd c
                                          then next c xs
                                          else send resp
          send resp = do lift $ contentsConns %= M.adjust (set _2 (Just resp)) i
                         return resp
  withContentsConnection n act = act (Name n)

instance MarquiseReaderMonad Store Name where
  sendReaderRequest op _ i = lift $ pointsConns %= M.insert i (op, Nothing)
  recvReaderResponse i = do
    conn <- lift $ use pointsConns >>= return . M.lookup i
    maybe (throwError $ Other "no connection")
          (\c -> case fst c of SimpleReadRequest a s e -> lift (use points) >>= next c a s e)
          conn
    where next _ _ _ _ [] = return EndOfStream
          next _ _ _ _ (FakeError:_) = get >>= throwError . Timeout
          next c a s e (Data (burst, SimplePoint a' t _):xs)
            = let resp = SimpleStream burst
              in  if   or [a /= a', t < s, t >= e, Just resp == snd c]
                  then next c a s e xs
                  else send resp
          send resp = do lift $ pointsConns %= M.adjust (set _2 (Just resp)) i
                         return resp
  withReaderConnection n act = act (Name n)
