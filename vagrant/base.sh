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

if [ $(cat /etc/system-release | grep -oE 'Fedora release [[:digit:]]+') == 'Fedora release 21' ]
then #working with fedora 21
    if [ -z `which javac` ]
    then
      # oracle jdk installation
      yum install java-1.8.0-openjdk java-1.8.0-openjdk-devel -y
    fi
else # working with ubuntu. Default

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
fi

chmod a+rw /opt
if [ ! -e /opt/kafka ]; then
    ln -s /vagrant /opt/kafka
fi
