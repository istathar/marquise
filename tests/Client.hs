{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

import Pipes
import Pipes.Internal
import qualified Pipes.Prelude as P
import Control.Monad.State
import Data.Maybe
import Test.Hspec
import qualified Data.Map as M
import qualified Data.List as L

import Marquise.Client
import Marquise.Types
import Store

instance Show (c -> Result a m c) where
  show _ = "a resumption"

main :: IO ()
main = hspec readTests

readTests :: Spec
readTests = do
  describe "Read: timeouts and recovers" $
    it "yields some points and returns a resumption pipe that can yield the rest" $ do
      v  <- withMarquiseHandler (error . show) vault
      vt <- withMarquiseHandler (error . show) vaultWithTimeout

      let (result0, resume0) = run v $ readSimplePointsResume addr start end org
      resume0 `shouldSatisfy` isNothing

      let (result1, resume1) = run vt $ readSimplePointsResume addr start end org
      resume1 `shouldSatisfy` isJust

      let (result2, _)       = run v  $ fromJust resume1

      result0 `shouldBe` (result1 ++ result2)

  where vault
          = simples >>= \x -> return $ PureStore [] x M.empty M.empty
        vaultWithTimeout
          = simples >>= \x -> let (y,z) = L.splitAt 1 x
                              in  return $ PureStore [] (y ++ [FakeError] ++ z) M.empty M.empty
        simples
          = let bursts = map SimpleBurst ["BBBBBBBBAAAAAAAACCCCCCCC", "DBBBBBBBAAAAAAAACCCCCCCC"]
            in  do ps <- P.toListM $ each bursts >-> decodeSimple
                   return $ map Data $ zip bursts ps
        start = TimeStamp 0
        end   = read "2500-01-01"
        addr  = read ""
        org   = read ""
        run ps act = flip evalState ps
                   $ store
                   $ withMarquiseHandler (error . show)
                   $ withReaderConnectionT "user"
                   $ \conn -> toListM' (_result $ act conn)

-- | Converts an effectful producer to a list, preserving its return value.
toListM' :: Monad m => Producer a m r -> m ([a], r)
toListM' = loop
  where loop p = case p of
          Request v _  -> closed v
          Respond a fu -> do
              (as, r) <- loop (fu ())
              return ((a:as), r)
          M         m  -> m >>= loop
          Pure      r  -> return ([], r)
