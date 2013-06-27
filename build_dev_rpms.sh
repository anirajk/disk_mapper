#!/bin/bash

#   Copyright 2013 Zynga Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

dm_server="netops-demo-mb-210.va2.zynga.com"
ss_server="netops-demo-mb-211.va2.zynga.com,netops-demo-mb-212.va2.zynga.com,netops-demo-mb-213.va2.zynga.com"
#dm_server="10.36.172.154"
#ss_server="10.36.161.172,10.36.160.34,10.36.162.35"

sudo rm -rf /root/rpmbuild/RPMS/x86_64/disk-mapper*
sudo rm -rf /root/rpmbuild/RPMS/x86_64/storage-server*
cd /home/sqadir/disk_mapper/dm_server
dm_rpm=$(sudo make rpm | grep Wrote: | grep x86_64 | awk '{print $NF}') 
dm_rpm_name=$(echo $dm_rpm | cut -d "/" -f 6)
rm -rf diskmapper.tgz

cd /home/sqadir/disk_mapper/storage_server
ss_rpm=$(sudo make rpm | grep Wrote: | grep x86_64 | awk '{print $NF}') 
ss_rpm_name=$(echo $ss_rpm | cut -d "/" -f 6)
rm -rf storageserver.tgz

echo Disk Mapper RPM : $dm_rpm
echo Storage Server RPM : $ss_rpm

cd /tmp/
sudo cp $dm_rpm .
sudo cp $ss_rpm .

sudo chmod 777 $dm_rpm_name
sudo chmod 777 $ss_rpm_name

scp $dm_rpm_name $dm_server:

for ip in $(echo $ss_server | sed "s/,/\n/g" ) ; do scp $ss_rpm_name $ip: ; done
