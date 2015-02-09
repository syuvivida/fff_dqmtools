#!/bin/bash -e

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILDDIR="$SCRIPTDIR/tmp/"

echo "Our directory: $SCRIPTDIR"
echo "Creating build dir: $BUILDDIR"
rm -fr $BUILDDIR
mkdir -p $BUILDDIR
cd $BUILDDIR

cat > fff-dqmtools.spec <<EOF
Name: fff-dqmtools
Version: 1.0.1
Release: 1
Summary: DQM tools for FFF.
License: gpl
Group: DQM
Packager: micius
Source: none
%define _tmppath $BUILDDIR/build-tmp
BuildRoot: %{_tmppath}
BuildArch: x86_64
AutoReqProv: no
Provides:/opt/fff_dqmtools
Provides:/etc/logrotate.d/fff_dqmtools
Provides:/etc/init.d/fff_monitoring
Requires:python, python-gevent
%description
DQM tools for FFF.
%prep
%build
%install

mkdir -p \$RPM_BUILD_ROOT/opt/fff_dqmtools
mkdir -p \$RPM_BUILD_ROOT/opt/fff_dqmtools/lib
mkdir -p \$RPM_BUILD_ROOT/opt/fff_dqmtools/static
mkdir -p \$RPM_BUILD_ROOT/etc/logrotate.d
mkdir -p \$RPM_BUILD_ROOT/etc/init.d
mkdir -p \$RPM_BUILD_ROOT/var/lib/fff_dqmtools

install -m 755 $SCRIPTDIR/*.py -t \$RPM_BUILD_ROOT/opt/fff_dqmtools/
install -m 644 $SCRIPTDIR/lib/*.py -t \$RPM_BUILD_ROOT/opt/fff_dqmtools/lib/
cp -r $SCRIPTDIR/static -t \$RPM_BUILD_ROOT/opt/fff_dqmtools/
cp -r $SCRIPTDIR/misc -t \$RPM_BUILD_ROOT/opt/fff_dqmtools/

install -m 755 $SCRIPTDIR/misc/fff_monitoring -t \$RPM_BUILD_ROOT/etc/init.d/
#install -m 755 $SCRIPTDIR/misc/fff_deleter -t \$RPM_BUILD_ROOT/etc/init.d/
install -m 644 $SCRIPTDIR/misc/fff_dqmtools.logrotate \$RPM_BUILD_ROOT/etc/logrotate.d/fff_dqmtools

%files
%defattr(-, root, root, -)
/opt/fff_dqmtools
/var/lib/fff_dqmtools
/etc/logrotate.d/fff_dqmtools
/etc/init.d/fff_monitoring

%post
/usr/lib/lsb/install_initd /etc/init.d/fff_monitoring
/etc/init.d/fff_monitoring restart
EOF

mkdir -p RPMBUILD/{RPMS/{noarch},SPECS,BUILD,SOURCES,SRPMS}
rpmbuild --define "_topdir `pwd`/RPMBUILD" -bb fff-dqmtools.spec
#rm -rf patch-cmssw-tmp
