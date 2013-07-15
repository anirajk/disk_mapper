FILES=__init__.py\
 request_handler.py\
 hook.sh\
 hook_complete.sh\
 hook_error.sh\
 disk_reinitialize.sh\
 resume_coalescer.py\
 lib/storageserver.py\
 lib/urlmapper.py\
 config/http_zbase_backup.conf\
 storage-server.spec\
 ../zstore_cmd/zstore_cmd\
 ../packages/urlrelay-0.7.1.tar.bz2\
 ../packages/BitTornado-0.3.17.tar.gz\
 Makefile


all:
	@echo "What do you want me to do ?"

tarball: 
	tar -czvf storageserver.tgz $(FILES)

rpm: tarball
	rpmbuild -ta storageserver.tgz
