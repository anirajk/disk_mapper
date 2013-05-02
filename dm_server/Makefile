FILES=__init__.py\
 request_handler.py\
 config.py\
 diskmapper_daemon.py\
 remove_host.py\
 lib/diskmapper.py\
 lib/flock.py\
 lib/urlmapper.py\
 config/http_disk_mapper.conf\
 init.d/disk_mapper\
 disk-mapper.spec\
 ../zstore_cmd/zstore_cmd\
 ../packages/urlrelay-0.7.1.tar.bz2\
 Makefile


all:
	@echo "What do you want me to do ?"

tarball: 
	tar -czvf diskmapper.tgz $(FILES)

rpm: tarball
	rpmbuild -ta diskmapper.tgz
