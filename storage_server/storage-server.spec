%define pkg_version 1.0
%define branch_version 2
%define _unpackaged_files_terminate_build 0

Summary:       Setup storage server
Name:          storage-server
Version:       %{pkg_version}
Release:       %{branch_version}
Group:         Servers/Internet
Source:        storageserver.tgz
BuildRoot:     %{_tmppath}/%{name}-%{version}-%{release}-root
Requires:      python httpd mod_wsgi ztorrent-client ztorrent-tracker
License:       Apache 2.0

%description
Setup NetOps storage server to backup up data.

%prep
%setup -q -c storage-server-%{version}


%install
%{__rm} -rf %{buildroot}
%{__mkdir_p} \
    %{buildroot}/tmp/ \
    %{buildroot}/etc/httpd/conf.d/ \
    %{buildroot}/etc/cron.d/ \
    %{buildroot}/var/www/html/zbase_backup/ \
    %{buildroot}/opt/storage_server/lib/ \
    %{buildroot}/usr/bin/

%{__cp} __init__.py request_handler.py resume_coalescer.py hook.sh hook_complete.sh disk_reinitialize.sh hook_error.sh %{buildroot}/opt/storage_server/
%{__cp} __init__.py lib/storageserver.py lib/urlmapper.py %{buildroot}/opt/storage_server/lib/
%{__cp} packages/urlrelay-0.7.1.tar.bz2 %{buildroot}/tmp/
%{__cp} packages/BitTornado-0.3.17.tar.gz %{buildroot}/tmp/
%{__chmod} +x  %{buildroot}/opt/storage_server/*.py
%{__chmod} +x  %{buildroot}/opt/storage_server/*.sh
%{__chmod} +x  %{buildroot}/opt/storage_server/lib/*.py

%{__cp} config/http_zbase_backup.conf %{buildroot}/etc/httpd/conf.d/zbase_backup.conf
%{__cp} utils/backup-purge.cron %{buildroot}/etc/cron.d/
%{__cp} zstore_cmd/zstore_cmd %{buildroot}/usr/bin/
%{__cp} backup_purger.sh %{buildroot}/usr/bin/
%{__chmod} +x  %{buildroot}/usr/bin/zstore_cmd
%{__chmod} +x  %{buildroot}/usr/bin/backup_purger.sh


%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-, storageserver, storageserver, 0755)
/opt/storage_server/*.sh
/opt/storage_server/*.py
/opt/storage_server/lib/*.py

%defattr(-, root, root, 0755)
/etc/httpd/conf.d/zbase_backup.conf
/usr/bin/zstore_cmd
/tmp/BitTornado-0.3.17.tar.gz
/tmp/urlrelay-0.7.1.tar.bz2

%post

# Install url_relay
cd /tmp/
tar -xvf /tmp/urlrelay-0.7.1.tar.bz2
cd urlrelay-0.7.1/
python setup.py install

# Setup ztorrent
pear install Net_URL2-0.3.1
pear install HTTP_Request2-2.0.0RC1
pear install Cache_Lite-1.7.11
pear install XML_RPC2

inf=bond0
ifconfig bond0
if [ $? -ne 0 ];
then
    inf=eth0
fi

ip=$(ifconfig $inf | grep -w inet | awk '{print $2}' | sed "s/.*://");
sed -i "s/@@TRACKER_IP_HERE@@/$ip/" /etc/opentracker/opentracker.conf
/etc/init.d/opentracker start

cd /tmp/
tar -xvf /tmp/BitTornado-0.3.17.tar.gz
cd BitTornado-CVS/
python setup.py install

sed -i "s/from sha import sha/from hashlib import sha1 as sha/" /usr/lib/python2.6/site-packages/BitTornado/BT1/makemetafile.py
sed -i "s/from sha import sha/from hashlib import sha1 as sha/" /usr/lib/python2.6/site-packages/BitTornado/__init__.py

# Setup symlinks to partition.
mkdir /var/www/html/zbase_backup
for part in `df | grep "/data_" |  awk '{print $NF}' ` ; do  ln -s $part /var/www/html/zbase_backup$part ; chmod -R 0777 $part ; done

# Create primary and secondary on each disk
for disk in `df -h | grep data_ | awk '{print $NF}' ` ; do mkdir $disk/primary ; mkdir $disk/secondary ; done
chown -R storageserver /data_*/
chown -R storageserver /var/www/html/
chmod -R a+x /var/www/html/

# Create bad file.
mkdir /var/tmp/disk_mapper
touch /var/tmp/disk_mapper/bad_disk
touch /var/tmp/disk_mapper/copy_completed
chown -R storageserver /var/tmp/disk_mapper

# Log file.
touch /var/log/storage_server.log
chown storageserver /var/log/storage_server.log

# Restart apache.
sed -i -e "s/User apache/User storageserver/" -e "s/Group apache/Group storageserver/" /etc/httpd/conf/httpd.conf
/etc/init.d/httpd restart

%preun

%postun
rm -rf /etc/httpd/conf.d/zbase_backup.conf



%changelog
* Tue Oct 01 2013 - akalathel@zynga.com
- Fixing add_entry api for copy_completed

* Mon Sep 30 2013 - akalathel@zynga.com
- Adding purger cron. adding file cleanup script

* Tue Dec 11 2012 - sqadir@zynga.com
- SEG-10543 - Remove torrent file, if pausing coalescer fails.

* Wed Dec 2 2012 - sqadir@zynga.com
- SEG-10426 - Added function to return game_id for a given host_name.

* Wed Nov 21 2012 - sqadir@zynga.com
- Fixing filename not appended to url issue (zstore_cmd)

* Wed Nov 21 2012 - sqadir@zynga.com

- Bug fixes.
- SEG-10395 - Torrents not getting created, when coalescers are not installed.

* Tue Oct 09 2012 - sqadir@zynga.com
- Initial QA Release

