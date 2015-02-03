# Marquise [![Build Status](https://travis-ci.org/anchor/marquise.svg?branch=master)](https://travis-ci.org/anchor/marquise)

Dependencies
------------

 - [vaultaire-common](https://github.com/anchor/vaultaire-common)

Overview
--------

Marquise is a collection of a library and two executables for use with Vaultaire.

* A client and server library for reading/writing to the vault and spool files.
  This provides streaming reads and writes to the vault using
  [pipes](https://hackage.haskell.org/package/pipes) as well as writing to spool
  files with automatic caching and rotation.

* An executable `marquised`, a daemon which writes data to the vault from spool
  files generated from users of the marquise library.

* An executable `data`, used for easily inspecting data in the vault as well as
  marquise cache files.


Installation + Deployment
-------------------------

Marquise is not currently on hackage but has no special requirements for a
Haskell package bar a system dependency on zeromq version >= 4.

[haskell2package](https://github.com/anchor/haskell2package) can create a
rpm for use with CentOS systems. Otherwise the recommended installation
method is with a cabal sandbox.

1. Install system dependencies.

    E.g. for debian-like systems:

    ```
    sudo apt-get install libzmq3-dev
    ```

    Ensure the version installed is >= 4, regardless of the name of the package.

1. Acquire and install dependencies and package from source.
    ```
    git clone git@github.com:anchor/vaultaire-common.git
    git clone git@github.com:anchor/marquise.git
    cd marquise
    cabal sandbox init
    cabal sandbox add-source ../vaultaire-common
    cabal install --only-dependencies -j && cabal build
    ```

1. For marquised: ensure relevant directories exist and are writeable by
   the user `marquised` will run as: `/var/{spool,cache}/marquise`

1. Run. The executables will be `dist/build/marquised/marquised` and `dist/build/data/data`.
