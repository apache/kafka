#!/bin/bash

set -eu

export PATH=$SNAP/usr/lib/jvm/default-java/bin:$PATH

$SNAP/opt/kafka/bin/$_wrapper_script "$@"
