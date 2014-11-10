Name:	        marquised
Version:	2.8.1
Release:	0anchor1%{?dist}
Summary:	marquised is a daemon for writing to Vaultaire.

Group:		Development/Libraries
License:	BSD
URL:		https://github.com/anchor/marquise
Source0:	marquise-%{version}.tar.gz
Source1:	vaultaire-common-2.6.0.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:	ghc >= 7.8.3
BuildRequires:  cabal-install
BuildRequires:  zeromq-devel >= 4.0.3
Requires:	gmp
Requires:	zeromq >= 4.0.3

%description
marquised is a daemon for writing to Vaultaire.

It reads from spool files written by collectors (using the Marquise
Haskell module or the C library libmarquise), and sends to a Vaultaire
broker via ZeroMQ.

%global ghc_without_dynamic 1

%prep
%setup -n vaultaire-common -T -b 1
export LC_ALL=en_US.UTF-8
cabal update
cabal install pipes-attoparsec
cabal install
%setup -n marquise


%build
export LC_ALL=en_US.UTF-8
cabal install
cabal build


%install
mkdir -p %{buildroot}/usr/bin
cp -v %{_builddir}/marquise/dist/build/marquised/marquised %{buildroot}%{_bindir}

%files
%defattr(-,root,root,-)

%{_bindir}/marquised


%changelog
* Wed Sep 24 2014 Sharif Olorin <sio@tesser.org> - 2.8.1-0anchor1
- rebuild with even more debug output

* Thu Sep 11 2014 Sharif Olorin <sio@tesser.org> - 2.7.0-0anchor3
- rebuild with even more debug output

* Wed Sep 10 2014 Sharif Olorin <sio@tesser.org> - 2.7.0-0anchor2
- rebuild with more debug output

* Tue Sep 09 2014 Sharif Olorin <sio@tesser.org> - 2.7.0-0anchor1
- bump to 2.7.0

* Mon Aug 25 2014 Sharif Olorin <sio@tesser.org> - 2.6.0-0anchor1
- initial build
