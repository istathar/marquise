# Release Notes
## 3.1.0
Expose some friendly wrappers for the client interface:

```
-- Supply an error handler to capture all errors from the Marquise monad.
withMarquiseHandler :: Monad m => (MarquiseErrorType -> m a) -> Marquise m a -> m a

-- Crash on all Marquise errors.
crashOnMarquiseErrors :: Monad m => Marquise m a -> m a

-- Ignore Marquise errors, for computations that do not have a return value.
ignoreMarquiseErrors :: Monad m => Marquise m () -> m ()

```

Example usage:

```
withReaderConnection broker $ \conn -> crashOnMarquiseErrors $ runEffect $ readSimple ...

```

## 3.0.0

The client interface changed slightly:

  * The return type changed from ``m a`` to ``Marquise m a`` for all client operations.
  * New operations: ``enumerateOriginResume``, ``readSimpleResume`` and ``readExtendedResume``
    are resumeable operations that return a continuation which can be run to acquire the rest of the data.
  * ``enumerateOrigin`` changes from

    ```
    enumerateOrigin :: MarquiseContentsMonad m conn => Origin -> conn
                    -> Producer (Address, SourceDict) m ()
    ```

    to

    ```
    enumerateOrigin :: MarquiseContentsMonad m conn => Policy -> Origin -> conn
                    -> Producer (Address, SourceDict) (Marquise m) ()
    ```

    where ``Policy`` are retry policies: ``NoRetry``, ``ForeverRetry`` and ``JustRetry <n>``.

  * Likewise ``readSimplePoints`` and ``readExtendedPoints`` have a retry policy.


Existing error behaviour (i.e. crash on all exceptions) can be emulated by changing any existing:

```
withReaderConnection broker $ \conn -> runEffect $ readSimple ...
```

to:

```
withReaderConnection broker $ \conn -> withMarquieHandler (error . show) $ runEffect $ readSimple ...
```

Example usage of a manual resume:

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