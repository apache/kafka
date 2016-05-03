# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

set -e

if [ -z `which javac` ]; then
    apt-get -y update
    apt-get install -y software-properties-common python-software-properties
    add-apt-repository -y ppa:webupd8team/java
    apt-get -y update

    # Try to share cache. See Vagrantfile for details
    mkdir -p /var/cache/oracle-jdk7-installer
    if [ -e "/tmp/oracle-jdk7-installer-cache/" ]; then
        find /tmp/oracle-jdk7-installer-cache/ -not -empty -exec cp '{}' /var/cache/oracle-jdk7-installer/ \;
    fi

    /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
    apt-get -y install oracle-java7-installer oracle-java7-set-default

    if [ -e "/tmp/oracle-jdk7-installer-cache/" ]; then
        cp -R /var/cache/oracle-jdk7-installer/* /tmp/oracle-jdk7-installer-cache
    fi
fi

chmod a+rw /opt
if [ -h /opt/kafka-trunk ]; then
    # reset symlink
    rm /opt/kafka-trunk
fi
ln -s /vagrant /opt/kafka-trunk

get_kafka() {
    version=$1

    kafka_dir=/opt/kafka-$version
    url=https://s3-us-west-2.amazonaws.com/kafka-packages-$version/kafka_2.10-$version.tgz
    if [ ! -d /opt/kafka-$version ]; then
        pushd /tmp
        curl -O $url
        file_tgz=`basename $url`
        tar -xzf $file_tgz
        rm -rf $file_tgz

        file=`basename $file_tgz .tgz`
        mv $file $kafka_dir
        popd
    fi
}

get_kafka 0.8.2.2
chmod a+rw /opt/kafka-0.8.2.2
get_kafka 0.9.0.1
chmod a+rw /opt/kafka-0.9.0.1

# For EC2 nodes, we want to use /mnt, which should have the local disk. On local
# VMs, we can just create it if it doesn't exist and use it like we'd use
# /tmp. Eventually, we'd like to also support more directories, e.g. when EC2
# instances have multiple local disks.
if [ ! -e /mnt ]; then
    mkdir /mnt
fi
chmod a+rwx /mnt

# Run ntpdate once to sync to ntp servers
# use -u option to avoid port collision in case ntp daemon is already running
ntpdate -u pool.ntp.org
# Install ntp daemon - it will automatically start on boot
apt-get -y install ntp
