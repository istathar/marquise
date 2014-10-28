{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Marquise.Classes
import Marquise.Client
import Marquise.Types
import Test.Hspec

-- we assume successes in this test, tests for failures (timeouts etc) need to be added

ns1, ns2 :: Monad m => m SpoolName
ns1 = withMarquiseHandler (error . show) $ makeSpoolName "ns1"
ns2 = withMarquiseHandler (error . show) $ makeSpoolName "ns2"

main :: IO ()
main = hspec suite

suite :: Spec
suite =
    describe "IO MarquiseClientMonad and MarquiseServerMonad" $
        it "reads appends, then cleans up when nextBurst is called" $ do
            (bytes1, bytes2, contents) <- withMarquiseHandler (error . show) $ do
              sf1 <- createSpoolFiles "ns1"
              sf2 <- createSpoolFiles "ns2"

              appendPoints sf1 "BBBBBBBBAAAAAAAACCCCCCCC"
              appendPoints sf1 "DBBBBBBBAAAAAAAACCCCCCCC"
              appendPoints sf2 "FBBBBBBBAAAAAAAACCCCCCCC"
              appendContents sf1 "contents"

              (bytes1,close_f1)   <- loop (nextPoints =<< ns1)
              (bytes2,close_f2)   <- loop (nextPoints =<< ns2)
              (contents,close_f3) <- loop (nextContents =<< ns1)

              catchTryIO_ $ do
                close_f1
                close_f2
                close_f3

              return (bytes1, bytes2, contents)

            bytes1 `shouldBe` "BBBBBBBBAAAAAAAACCCCCCCC\
                              \DBBBBBBBAAAAAAAACCCCCCCC"
            bytes2 `shouldBe` "FBBBBBBBAAAAAAAACCCCCCCC"
            contents `shouldBe` "contents"

  where
    loop x = do
        x' <- x
        case x' of
            Just result -> return result
            Nothing -> loop x
