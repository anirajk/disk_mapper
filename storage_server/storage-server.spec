%define pkg_version 1.0.0
%define branch_version 0.5

Summary:       Setup storage server
Name:          storage-server
Version:       %{pkg_version}
Release:       %{branch_version}
Group:         Servers/Internet
Source:        storageserver.tgz
BuildRoot:     %{_tmppath}/%{name}-%{version}-%{release}-root
Requires:      php python httpd mod_wsgi php-common php-pear php-devel php-cli php-pdo openssl openssl-devel openssl098e openssl-static openssl-perl ztorrent-client ztorrent-tracker
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
    %{buildroot}/var/www/html/membase_backup/ \
    %{buildroot}/opt/storage_server/lib/ \
    %{buildroot}/usr/bin/

%{__cp} __init__.py request_handler.py resume_coalescer.py hook.sh %{buildroot}/opt/storage_server/
%{__cp} __init__.py lib/storageserver.py lib/urlmapper.py %{buildroot}/opt/storage_server/lib/
%{__cp} packages/urlrelay-0.7.1.tar.bz2 %{buildroot}/tmp/
%{__cp} packages/BitTornado-0.3.17.tar.gz %{buildroot}/tmp/
%{__chmod} +x  %{buildroot}/opt/storage_server/*.py
%{__chmod} +x  %{buildroot}/opt/storage_server/*.sh
%{__chmod} +x  %{buildroot}/opt/storage_server/lib/*.py

%{__cp} config/http_membase_backup.conf %{buildroot}/etc/httpd/conf.d/membase_backup.conf

%{__cp} zstore_cmd/zstore_cmd %{buildroot}/usr/bin/
%{__chmod} +x  %{buildroot}/usr/bin/zstore_cmd


%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-, apache, apache, 0755)
/opt/storage_server/*.sh
/opt/storage_server/*.py
/opt/storage_server/lib/*.py

%defattr(-, root, root, 0755)
/etc/httpd/conf.d/membase_backup.conf
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



ip=$(ifconfig eth0 | grep -w inet | awk '{print $2}' | sed "s/.*://") ; sed -i "s/@@TRACKER_IP_HERE@@/$ip/" /etc/opentracker/opentracker.conf
/etc/init.d/opentracker start

cd /tmp/
tar -xvf /tmp/BitTornado-0.3.17.tar.gz
cd BitTornado-CVS/
python setup.py install

sed -i "s/from sha import sha/from hashlib import sha1 as sha/" /usr/lib/python2.6/site-packages/BitTornado/BT1/makemetafile.py
sed -i "s/from sha import sha/from hashlib import sha1 as sha/" /usr/lib/python2.6/site-packages/BitTornado/__init__.py

# Setup symlinks to partition.
mkdir /var/www/html/membase_backup
for part in `df | grep "/data_" |  awk '{print $NF}' ` ; do  ln -s $part /var/www/html/membase_backup$part ; chmod -R 0777 $part ; done

# Create primary and secondary on each disk
for disk in `df -h | grep data_ | awk '{print $NF}' ` ; do mkdir $disk/primary ; mkdir $disk/secondary ; done
chown -R apache /data_*/
chown -R apache /var/www/html/
chmod -R a+x /var/www/html/

# Create bad file.
mkdir /var/tmp/disk_mapper
touch /var/tmp/disk_mapper/bad_disk
chown -R apache /var/tmp/disk_mapper

# Restart apache.
/etc/init.d/httpd restart

# Added blobrestore user.
useradd blobrestore
mkdir -p /home/blobrestore/.ssh
echo 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDNgpjkGUs+bGjYYsLHMrfdtJedQY58uyYMADDqPGG+0IWVKwlTxQ1RltAWWTpv7Dr0DCIM4OgLq5iyD0utUt4V5cFX+2niXojTqYQfE4B6hYch75BRr3EqkQLuFrUVd77gs6dp7cwQORsOudmxqn42QUrrtvMub8wvDs9kJEMz65MtuxEAy9bRNZy09HTR8qAle8HfEyl8JoRunhtSDQoPtZVFfs7/L0VIp1tb4Q1bNEhVS7RbYJawt59rn1RpBMddBcYy13QZv+KHTli2/FSga0EbodvJBFgIQfg3/4t0pzAGuw7psEuHRgpLQBtp197l9SXYGxJyb7Ayfw6feNsH' > /home/blobrestore/.ssh/authorized_keys
chmod 755 /home/blobrestore
chmod 700 /home/blobrestore/.ssh
chmod 600 /home/blobrestore/.ssh/authorized_keys
chown -R blobrestore /home/blobrestore/


%preun

%postun



%changelog

* Wed Nov 21 2012 - sqadir@zynga.com
- Fixing filename not appended to url issue (zstore_cmd)

* Wed Nov 21 2012 - sqadir@zynga.com

- Bug fixes.
- SEG-10395 - Torrents not getting created, when coalescers are not installed.

* Tue Oct 09 2012 - sqadir@zynga.com
- Initial QA Release

