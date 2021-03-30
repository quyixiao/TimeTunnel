##############################################################
# http://twiki.corp.alimama.com/twiki/bin/view/Alimm_OPS/RPM #
# http://www.rpm.org/max-rpm/ch-rpm-inside.html              #
##############################################################
Name: t_dp_timetunnel_dfswriter
Version: 0.2.0
# if you want get version number from outside, use like this
#Version: %(cat version.txt)
Release: 1
# if you want use the parameter of rpm_create on build time,
# uncomment below
#Release: %{_release}
Summary: timetunnel2 save file rpm package
# this is the svn URL of current dir
URL: %{_svn_path}
Group: TimeTunnel/SaveFile
License: Commercial

# uncomment below, if you want rpmbuild don't strip files
#%define __spec_install_port /usr/lib/rpm/brp-compress
#%define __os_install_post /usr/lib/rpm/brp-compress

# uncomment below, if your files don't depend on 32/64bit OS,
# such as script, config or data files

BuildArch: noarch

# uncomment below, if depend on other package

%description
# if you want publish current svn URL or Revision use these macros
%{_svn_path}
%{_svn_revision}
timetunnel dfswriter rpm package version 1.0.0

%debug_package
# support debuginfo package, to reduce runtime package size

# prepare your files
%install
# OLDPWD is the dir of rpm_create running
# _prefix is an inner var of rpmbuild,
# can set by rpm_create, default is "/home/a"
# _lib is an inner var, maybe "lib" or "lib64" depend on OS

# create dirs
mkdir -p .%{_prefix}/dfswriter/lib/
mkdir -p .%{_prefix}/dfswriter/conf/
mkdir -p .%{_prefix}/dfswriter/log/
mkdir -p .%{_prefix}/dfswriter/bin/

# copy files
cp $OLDPWD/../target/dist/lib/* .%{_prefix}/dfswriter/lib/
cp $OLDPWD/../target/dist/conf/* .%{_prefix}/dfswriter/conf/
cp $OLDPWD/../target/dist/bin/* .%{_prefix}/dfswriter/bin/

# create a crontab of the package
#echo "
#* * * * * root /home/a/bin/every_min
#3 * * * * ads /home/a/bin/every_hour
#" > %{_crontab}

# package infomation

%files
# set file attribute here
%defattr(-,admin,admin)
%attr(0755,admin,admin) %{_prefix}/dfswriter/bin/start.sh
%attr(0755,admin,admin) %{_prefix}/dfswriter/bin/stop.sh

# need not list every file here, keep it as this
%{_prefix}
# create an empy dir
%dir %{_prefix}/dfswriter/log/
%config(noreplace) %{_prefix}/dfswriter/conf/*

# need bakup old config file, so indicate here
#%config %{_prefix}/etc/sample.conf
# or need keep old config file, so indicate with "noreplace"
#%config(noreplace) %{_prefix}/etc/sample.conf
# indicate the dir for crontab
%{_crondir}

%changelog
#
