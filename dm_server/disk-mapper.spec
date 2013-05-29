%define pkg_version 1.0
%define branch_version 2
%define _unpackaged_files_terminate_build 0

Summary:       Disk Mapper
Name:          disk-mapper
Version:       %{pkg_version}
Release:       %{branch_version}
Group:         Servers/Internet
Source:        diskmapper.tgz
BuildRoot:     %{_tmppath}/%{name}-%{version}-%{release}-root
Requires:      python httpd mod_wsgi
License:       Proprietary

%description
Setup NetOps storage server to backup up data.

%prep
%setup -q -c storage-server-%{version} 

%install
%{__rm} -rf %{buildroot}
%{__mkdir_p} \
    %{buildroot}/tmp/ \
    %{buildroot}/etc/httpd/conf.d/ \
    %{buildroot}/opt/disk_mapper/lib/ \
    %{buildroot}/opt/disk_mapper/ \
    %{buildroot}/etc/init.d/ \
    %{buildroot}/usr/bin/

%{__cp} __init__.py config.py diskmapper_daemon.py request_handler.py remove_host.py %{buildroot}/opt/disk_mapper/
%{__cp} __init__.py lib/flock.py lib/diskmapper.py lib/urlmapper.py %{buildroot}/opt/disk_mapper/lib/
%{__cp} init.d/disk_mapper %{buildroot}/etc/init.d/
%{__cp} packages/urlrelay-0.7.1.tar.bz2 %{buildroot}/tmp/
%{__chmod} +x  %{buildroot}/opt/disk_mapper/*.py
%{__chmod} +x  %{buildroot}/opt/disk_mapper/lib/*.py

%{__cp} config/http_disk_mapper.conf %{buildroot}/etc/httpd/conf.d/disk_mapper.conf

%{__cp} zstore_cmd/zstore_cmd %{buildroot}/usr/bin/
%{__chmod} +x  %{buildroot}/usr/bin/zstore_cmd

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-, apache, apache, 0755)
/opt/disk_mapper/__init__.py
/opt/disk_mapper/request_handler.py
/opt/disk_mapper/config.py
/opt/disk_mapper/remove_host.py
/opt/disk_mapper/lib/*.py

%defattr(-, root, root, 0755)
/etc/init.d/disk_mapper
/etc/httpd/conf.d/disk_mapper.conf
/opt/disk_mapper/diskmapper_daemon.py
/usr/bin/zstore_cmd
/tmp/urlrelay-0.7.1.tar.bz2

%post

# Install url_relay
cd /tmp/
tar -xvf /tmp/urlrelay-0.7.1.tar.bz2
cd urlrelay-0.7.1/
python setup.py install 

# Create disk mapper tmp folder
mkdir /var/tmp/disk_mapper
touch /var/tmp/disk_mapper/host.mapping
chown -R apache /var/tmp/disk_mapper

# Set perms to init.d script
chmod 755 /etc/init.d/disk_mapper

# Create Log file
touch /var/log/disk_mapper.log
chown apache /var/log/disk_mapper.log
chmod 0666 /var/log/disk_mapper.log


# Restart apache.
chown -R apache /var/www/html
/etc/init.d/httpd restart

%preun

%postun
kill -9 $(ps ax | grep diskmapper_daemon.py | grep -v grep | awk '{print $1}')
rm -rf /etc/httpd/conf.d/disk_mapper.conf

%changelog

* Tue Dec 11 2012 - sqadir@zynga.com
- SEG-10475 - Don't initialize disks for hosts that have primary and secondary disks as bad.

* Wed Dec 5 2012 - sqadir@zynga.com
- SEG-10428 - Use write locks.

* Sun Dec 2 2012 - sqadir@zynga.com
- SEG-10426 - Get game ID from replica, instead of host name.

* Wed Nov 22 2012 - sqadir@zynga.com
- Added function to list bad disk too, for QA automation.

* Wed Nov 21 2012 - sqadir@zynga.com
- Added function to list entire mapping for QA automation.

* Wed Nov 21 2012 - sqadir@zynga.com
- Changed to handle MCS key, only if its a string.

* Wed Nov 21 2012 - sqadir@zynga.com
- Bug fixes.
- SEG-10393 - Mapping not getting updated because of file sync issues.
- SEG-10391 - Unable to start disk mapper daemon.
- SEG-10398 - Disk mapper daemon crashing.

* Tue Oct 09 2012 - sqadir@zynga.com
- Initial QA Release

