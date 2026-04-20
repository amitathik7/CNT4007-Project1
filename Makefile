make:
	javac PeerProcess.java

clear:
	rm ./100* -fr
	rm *.log
	rm Common.cfg
	rm PeerInfo.cfg
	rm *.class

test:
	mkdir 1001
	mkdir 1002
	mkdir 1003
	mkdir 1004
	mkdir 1005
	mkdir 1006
	mkdir 1007
	mkdir 1008
	mkdir 1009

	touch log_peer_1001.log
	touch log_peer_1002.log
	touch log_peer_1003.log
	touch log_peer_1004.log
	touch log_peer_1005.log
	touch log_peer_1006.log
	touch log_peer_1007.log
	touch log_peer_1008.log
	touch log_peer_1009.log

	cp ./examples/project_config_file_small/Common.cfg ./Common.cfg
	cp ./examples/project_config_file_small/PeerInfo.cfg ./PeerInfo.cfg
	cp ./examples/project_config_file_small/1001/thefile ./1001/thefile
	cp ./examples/project_config_file_small/1006/thefile ./1006/thefile

	make
