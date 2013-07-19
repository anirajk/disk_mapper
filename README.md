disk_mapper
===========

Diskmapper is a vbucket/host level backup storage system. It keeps one primary
and secondary copy of backups. Each host is mapped to a disk. Each disk holds
one primary for a vbucket/host and secondary copy for another vbucket/host.
When disks become bad, it can automatically perform failover and map failed
disk's content to another disk based on spare availability.


Diskmapper service consists of dm_server and storage servers. dm_server is the
central server that makes the decision on which vbucket/host should be mapped
to which storage server. In a typical deployment, there should be one dm_server
and many storage servers. You can add more storage servers in need to the
existing cluster by making a config change in dm_server. The minimum number of
storageservers required for a cluster is two.


#### Dependencies
    - CentOS 6
    - httpd
    - mod_wsgi

#### Building dm_server and storage server

    $ cd dm_server
    $ make rpm

    $ cd storage_server
    $ make rpm


#### Installation

#####Installation of storage servers

Storage servers expects the disks to be mounted as /data_1, /data_2 ... /data_n
naming convention. For development purpose, easy faking can be done by mounting
tmpfs to similar mountpoints as follows:

    Create mount points
    # mkdir /data_{1,2,3}
    # for i in {1..3}; do mount -t tmpfs none /data_$i ; done

    Add storageserver user
    # useradd storageserver
    # chown storageserver /data_{1..3}

    # Clear apache webroot
    # rm -rf /var/www/html/*

    Install torrent packages
    # rpm -i packages/ztorrent-client-2.0-8.noarch.rpm
    # rpm -i packages/ztorrent-tracker-2.0-8.noarch.rpm

    Install storage server package
    # rpm -i storage_server.rpm

    Test your installation
    # curl localhost/api?action=get_config
    {"data_3": {"primary": "spare", "secondary": "spare"}, "data_2":
    {"primary": "spare", "secondary": "spare"}, "data_1": {"primary": "spare",
    "secondary": "spare"}}

    Install zbase backup tools
    Please build zbase-backup-tools from the corresponding repository and
    install on storage server and start backup daemon and merge daemon after
    adding proper config in /etc/zbase-backup/default.ini
    # /etc/init.d/backup_merged start
    # /etc/init.d/zbase_backupd start

##### Installation of dm_server

    Install dm_server package
    # rpm -i dm_server.rpm

    Edit config and add the list of storage servers to be tracked by dm_server
    and zruntime config
    # vim /opt/disk_mapper/config.py

    Start diskmapper service
    # /etc/init.d/disk_mapper start

    Test your installation
    A successful instation should return empty mapping as follows.
    # curl localhost/api?action=get_all_config
    {}


#### Allocation of vbuckets

    Initialize 32 vbuckets to disk mapper service as follows:
    # ./initialize_diskmapper.sh -i 0.0.0.0 -g zbase -v 10 -t 32

    Verify generated mapping
    # curl http://localhost/api?action=get_vb_mapping


#### Notes

    Disk mapper service can operate in vbucket level storage as well as host
    level storage. Disk mapper v2.0 supports only host level backup storage.

    To upload files in host level storage mode, zstore_cmd command can be used
    as follows:

    $ zstore_cmd/zstore_cmd put file s3://diskmapper_ip/namspace/hostname/subdirectories/file

    zstore_cmd also supports list, get, sync, del apis.

