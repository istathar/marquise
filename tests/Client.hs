import Pipes
import Pipes.Internal
import Control.Monad.State.Strict
import qualified Pipes.Prelude as P
import Test.Hspec

import Marquise.Client
import Marquise.Types
import Store


main = undefined

readTests :: Spec
readTests = do
  describe "Read: timeouts after a few points" $
    it "yields some points and returns a resumption pipe that can yield the rest" $ do
      (result0, resume0) <- run vault            $ readSimplePointsResume addr start end org
      (result1, resume1) <- run vaultWithTimeout $ readSimplePointsResume addr start end org
      (result2, _)       <- run vault            $ resume1

      resume0 `shouldBe` Nothing
      result0 `shouldBe` result1 ++ result2

  where vault = undefined
        vaultWithTimeout = undefined
        addr = undefined
        start = undefined
        end = undefined
        org = undefined
        run ps act = flip evalState ps
                   $ store
                   $ withMarquiseHandler (error . show)
                   $ withReaderConnectionT "user"
                   $ \conn -> toListM' $ _result $ act conn

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
