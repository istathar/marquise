{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Exception (throw)
import Marquise.Classes
import Marquise.Client
import Test.Hspec

ns1, ns2 :: SpoolName
ns1 = either throw id $ makeSpoolName "ns1"
ns2 = either throw id $ makeSpoolName "ns2"

main :: IO ()
main = hspec suite

suite :: Spec
suite =
    describe "IO MarquiseClientMonad and MarquiseServerMonad" $
        it "reads appends, then cleans up when nextBurst is called" $ do
            sf1 <- createSpoolFiles "ns1"
            sf2 <- createSpoolFiles "ns2"

            appendPoints sf1 "BBBBBBBBAAAAAAAACCCCCCCC"
            appendPoints sf1 "DBBBBBBBAAAAAAAACCCCCCCC"
            appendPoints sf2 "FBBBBBBBAAAAAAAACCCCCCCC"
            appendContents sf1 "contents"

            (bytes1,close_f1) <- nextPoints ns1
            (bytes2,close_f2) <- nextPoints ns2
            (contents,close_f3) <- nextContents ns1

            bytes1 `shouldBe` "BBBBBBBBAAAAAAAACCCCCCCC\
                              \DBBBBBBBAAAAAAAACCCCCCCC"
            bytes2 `shouldBe` "FBBBBBBBAAAAAAAACCCCCCCC"
            close_f1
            close_f2

            contents `shouldBe` "contents"
            close_f3
