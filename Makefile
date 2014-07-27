all: build

marquised: dist/build/marquised/marquised

#
# Setup
#

ifdef V
MAKEFLAGS=-R
else
MAKEFLAGS=-s -R
REDIRECT=2>/dev/null
endif

.PHONY: all build test



#
# Build rules. This just wraps Cabal doing its thing in a Haskell
# language-specific fashion.
#

build: dist/setup-config tags
	@/bin/echo -e "CABAL\tbuild"
	cabal build

test: dist/setup-config tags
	@/bin/echo -e "CABAL\ttest"
	cabal test

dist/setup-config: marquise.cabal Setup.hs
	cabal configure \
		--enable-tests \
		--enable-benchmarks \
		-v0 2>/dev/null || /bin/echo -e "CABAL\tinstall --only-dependencies" && cabal install --only-dependencies --enable-tests --enable-benchmarks
	@/bin/echo -e "CABAL\tconfigure"
	cabal configure \
		--enable-tests \
		--enable-benchmarks \
		--disable-library-profiling \
		--disable-executable-profiling


SOURCES=$(shell find src -name '*.hs' -type f)\
        $(shell find lib -name '*.hs' -type f)

# This will match writer-test/writer-test, so we have to strip the directory
# portion off. Annoying, but you can't use two '%' in a pattern rule.
dist/build/%: dist/setup-config tags $(SOURCES)
	@/bin/echo -e "CABAL\tbuild $@"
	cabal build $(notdir $@)

HOTHASKTAGS=$(shell which hothasktags 2>/dev/null)
CTAGS=$(if $(HOTHASKTAGS),$(HOTHASKTAGS),/bin/false)

tags: $(SOURCES)
	if [ "$(HOTHASKTAGS)" ] ; then /bin/echo -e "CTAGS\ttags" ; fi
	-$(CTAGS) $^ > tags $(REDIRECT)

format: $(SOURCES)
	stylish-haskell -i $^

clean:
	cabal clean
	-rm src/Package.hs
