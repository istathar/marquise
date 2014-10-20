# Release Notes
## 3.0

The server side is unchanged.

The client interface changed slightly. In particular the return type of all client operations changed from ``m a`` to ``Marquise m a``. For example:

```
readSimple :: MarquiseReaderMonad m conn -> .. -> Producer SimpleBurst m ()
```

is now:

```
readSimple :: MarquiseReaderMonad m conn -> .. -> Producer SimpleBurst (Marquise m) ()
```

Existing behaviour (i.e. crash on all exceptions) can be emulated by changing any existing:

```
withReaderConnection broker $ \conn ->
  runEffect $ readSimple ...
```

to:

```
withReaderConnection broker $ \conn ->
  withMarquieHandler (error . show) $ runEffect $ readSimple ...
```

Otherwise errors can be caught and handled inside the Marquise operation.
Three Marquise client operations, ``enumerateOrigin``, ``readSimple`` and ``readExtended`` now offer
resumeable versions to continue streaming if there is a **Timeout** or **ZMQ** error.


```
readSimple             :: ... -> Producer SimpleBurst (Marquise m) ()
readSimplePointsResume :: ... -> Result SimplePoint (Marquise m) conn
```

Example usage:


```
act :: IO ()
act = do
  x <- withReaderConnection broker
     $ \conn ->
     -- crash on errors that are not timeouts or ZMQ problems
     $ withMarquiseHandler (error . show) $ runEffect
     $ _result (readSimplePointsResume addr start end org conn)
   >-> ...
   >-> Pipes.print

  case x of Nothing     -> print "success"
            Just resume -> do print "try only one more time"
                              withReaderConnection broker $ \conn2 -> ...
                              $ _result (resume conn2) >-> ...
```
