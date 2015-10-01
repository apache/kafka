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

import os
import sys
import subprocess
import tempfile


class Keytool:

    @staticmethod
    def generate_keystore_truststore(dir='.'):
        """
        Generate JKS keystore and truststore and return
        Kafka SSL properties with these stores.
        """
        ksPath = os.path.join(dir, 'test.keystore.jks')
        ksPassword = 'test-ks-passwd'
        keyPassword = 'test-key-passwd'
        tsPath = os.path.join(dir, 'test.truststore.jks')
        tsPassword = 'test-ts-passwd'
        if os.path.exists(ksPath):
            os.remove(ksPath)
        if os.path.exists(tsPath):
            os.remove(tsPath)
        
        Keytool.runcmd("keytool -genkeypair -alias test -keyalg RSA -keysize 2048 -keystore %s -storetype JKS -keypass %s -storepass %s -dname CN=systemtest" % (ksPath, keyPassword, ksPassword))
        Keytool.runcmd("keytool -export -alias test -keystore %s -storepass %s -storetype JKS -rfc -file test.crt" % (ksPath, ksPassword))
        Keytool.runcmd("keytool -import -alias test -file test.crt -keystore %s -storepass %s -storetype JKS -noprompt" % (tsPath, tsPassword))
        os.remove('test.crt')

        return {
            'ssl.keystore.location' : ksPath,
            'ssl.keystore.password' : ksPassword,
            'ssl.key.password' : keyPassword,
            'ssl.truststore.location' : tsPath,
            'ssl.truststore.password' : tsPassword
        }

    @staticmethod
    def runcmd(cmd):
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


class SecurityConfig:

    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SSL_DIR = "/mnt/ssl"
    KEYSTORE_PATH = "/mnt/ssl/test.keystore.jks"
    TRUSTSTORE_PATH = "/mnt/ssl/test.truststore.jks"

    ssl_stores = Keytool.generate_keystore_truststore('.')

    def __init__(self, node_account, security_protocol, template_props=""):
        """
        Initialize the security properties for the node and copy
        keystore and truststore to the remote node if the transport protocol 
        is SSL. If security_protocol is None, the protocol specified in the
        template properties file is used. If no protocol is specified in the
        template properties either, PLAINTEXT is used as default.
        """

        self.node_account = node_account
        if security_protocol is None:
            for line in template_props.split("\n"):
                items = line.split("=")
                if len(items) == 2 and items[0].strip() == 'security.protocol':
                    security_protocol = str(items[1].strip())
                    if security_protocol not in [SecurityConfig.PLAINTEXT, SecurityConfig.SSL]:
                        raise Exception("Invalid security.protocol in template properties: " + security_protocol)
        if security_protocol is None:
            security_protocol = SecurityConfig.PLAINTEXT
        if security_protocol == SecurityConfig.SSL:
            node_account.ssh("mkdir -p %s" % SecurityConfig.SSL_DIR, allow_fail=False)
            node_account.scp_to(SecurityConfig.ssl_stores['ssl.keystore.location'], SecurityConfig.KEYSTORE_PATH)
            node_account.scp_to(SecurityConfig.ssl_stores['ssl.truststore.location'], SecurityConfig.TRUSTSTORE_PATH)

        self.properties = {
            'security.protocol' : security_protocol,
            'ssl.keystore.location' : SecurityConfig.KEYSTORE_PATH,
            'ssl.keystore.password' : SecurityConfig.ssl_stores['ssl.keystore.password'],
            'ssl.key.password' : SecurityConfig.ssl_stores['ssl.key.password'],
            'ssl.truststore.location' : SecurityConfig.TRUSTSTORE_PATH,
            'ssl.truststore.password' : SecurityConfig.ssl_stores['ssl.truststore.password']
        }
    
    def write_to_file(self, prop_file='/mnt/ssl/security.properties'):
        """Write security properties to a remote file"""
        tmpfile =  tempfile.NamedTemporaryFile(delete=False)
        localfile = tmpfile.name
        with open(localfile, 'w') as fd:
            for key, value in self.properties.items():
                fd.write(key + "=" + value + "\n")
        self.node_account.scp_to(localfile, prop_file)
        os.remove(localfile)
        return prop_file

    @property
    def security_protocol(self):
        return self.properties['security.protocol']

    def __str__(self):
        """
        Return properties as string with line separators.
        This is used to append security config properties to
        a properties file.
        """

        prop_str = ""
        if self.security_protocol == SecurityConfig.SSL:
            for key, value in self.properties.items():
                prop_str += ("\n" + key + "=" + value)
            prop_str += "\n"
        return prop_str

