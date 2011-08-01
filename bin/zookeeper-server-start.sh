#!/bin/bash

if [ $# -ne 1 ];
then
	echo "USAGE: $0 zookeeper.properties"
	exit 1
fi

$(dirname $0)/kafka-run-class.sh org.apache.zookeeper.server.quorum.QuorumPeerMain $@