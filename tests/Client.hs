{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad.State
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe
import Data.Monoid
import Pipes
import Pipes.Internal
import qualified Pipes.Prelude as P
import Test.Hspec

import Marquise.Client
import Marquise.Types
import Store

instance Show (c -> Result a m c) where
  show _ = "a resumption"

main :: IO ()
main = hspec readTests

readTests :: Spec
readTests = do
  v  <- withMarquiseHandler (error . show) vault
  vt <- withMarquiseHandler (error . show) vaultWithTimeout

  describe "Contents: timeouts and recovers" $ do
    it "checks that running the resumption after a timeout is equivalent to running the whole request without timeouts" $ do
      let (result0, resume0) = run0 v  $ enumerateOriginResume org
      putStrLn $ "result 0 " ++ show result0
      putStrLn $ "resume 0 " ++ show resume0

      resume0 `shouldSatisfy` isNothing

      let (result1, resume1) = run0 vt $ enumerateOriginResume org
      resume1 `shouldSatisfy` isJust

      let (result2, _)       = run0 v  $ fromJust resume1

      (result1 ++ result2) `shouldBe` result0

  describe "Read: timeouts and recovers" $
    it "checks that running the resumption after a timeout is equivalent to running the whole request without timeouts" $ do
      let (result0, resume0) = run1 v $ readSimplePointsResume addr start end org
      resume0 `shouldSatisfy` isNothing

      let (result1, resume1) = run1 vt $ readSimplePointsResume addr start end org
      resume1 `shouldSatisfy` isJust

      let (result2, _)       = run1 v  $ fromJust resume1

      (result1 ++ result2) `shouldBe` result0

  where vault
          = simples >>= \x -> return $ PureStore cs x M.empty M.empty

        vaultWithTimeout
          = simples >>= \x -> let (y,z) = L.splitAt 1 x
                                  (a,b) = L.splitAt 1 cs
                              in  return $ PureStore (a ++ [FakeError] ++ b)
                                                     (y ++ [FakeError] ++ z)
                                                      M.empty M.empty
        simples
          = let bursts = map SimpleBurst ["BBBBBBBBAAAAAAAACCCCCCCC", "DBBBBBBBAAAAAAAACCCCCCCC"]
            in  do ps <- P.toListM $ each bursts >-> decodeSimple
                   return $ map Data $ zip bursts ps

        cs = map Data [(read "ABCDEF", mempty), (read "DEADBEEF", mempty)]

        start = TimeStamp 0
        end   = read "2500-01-01"
        addr  = read ""
        org   = read ""

        run0 ps act = flip evalState ps $ store
                   $ withMarquiseHandler (error . show)
                   $ withContentsConnectionT "test0"
                   $ \conn -> toListM' (_result $ act conn)

        run1 ps act = flip evalState ps $ store
                   $ withMarquiseHandler (error . show)
                   $ withReaderConnectionT "test1"
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
