#!/bin/bash

set -eu

if [ ! -f $SNAP_DATA/server.properties ]; then
	cp $SNAP/opt/kafka/config/server.properties $SNAP_DATA/server.properties
fi

export PATH=$SNAP/usr/lib/jvm/default-java/bin:$PATH
export LOG_DIR=$SNAP_DATA/log

$SNAP/opt/kafka/bin/kafka-server-start.sh $SNAP_DATA/server.properties
