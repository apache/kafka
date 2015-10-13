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
import subprocess


class Keytool(object):

    @staticmethod
    def generate_keystore_truststore(ssl_dir='.'):
        """
        Generate JKS keystore and truststore and return
        Kafka SSL properties with these stores.
        """
        ks_path = os.path.join(ssl_dir, 'test.keystore.jks')
        ks_password = 'test-ks-passwd'
        key_password = 'test-key-passwd'
        ts_path = os.path.join(ssl_dir, 'test.truststore.jks')
        ts_password = 'test-ts-passwd'
        if os.path.exists(ks_path):
            os.remove(ks_path)
        if os.path.exists(ts_path):
            os.remove(ts_path)
        
        Keytool.runcmd("keytool -genkeypair -alias test -keyalg RSA -keysize 2048 -keystore %s -storetype JKS -keypass %s -storepass %s -dname CN=systemtest" % (ks_path, key_password, ks_password))
        Keytool.runcmd("keytool -export -alias test -keystore %s -storepass %s -storetype JKS -rfc -file test.crt" % (ks_path, ks_password))
        Keytool.runcmd("keytool -import -alias test -file test.crt -keystore %s -storepass %s -storetype JKS -noprompt" % (ts_path, ts_password))
        os.remove('test.crt')

        return {
            'ssl.keystore.location' : ks_path,
            'ssl.keystore.password' : ks_password,
            'ssl.key.password' : key_password,
            'ssl.truststore.location' : ts_path,
            'ssl.truststore.password' : ts_password
        }

    @staticmethod
    def runcmd(cmd):
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


class SecurityConfig(object):

    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SSL_DIR = "/mnt/ssl"
    KEYSTORE_PATH = "/mnt/ssl/test.keystore.jks"
    TRUSTSTORE_PATH = "/mnt/ssl/test.truststore.jks"

    ssl_stores = Keytool.generate_keystore_truststore('.')

    def __init__(self, security_protocol, template_props=""):
        """
        Initialize the security properties for the node and copy
        keystore and truststore to the remote node if the transport protocol 
        is SSL. If security_protocol is None, the protocol specified in the
        template properties file is used. If no protocol is specified in the
        template properties either, PLAINTEXT is used as default.
        """

        if security_protocol is None:
            security_protocol = self.get_property('security.protocol', template_props)
        if security_protocol is None:
            security_protocol = SecurityConfig.PLAINTEXT
        elif security_protocol not in [SecurityConfig.PLAINTEXT, SecurityConfig.SSL]:
            raise Exception("Invalid security.protocol in template properties: " + security_protocol)

        self.properties = {
            'security.protocol' : security_protocol,
            'ssl.keystore.location' : SecurityConfig.KEYSTORE_PATH,
            'ssl.keystore.password' : SecurityConfig.ssl_stores['ssl.keystore.password'],
            'ssl.key.password' : SecurityConfig.ssl_stores['ssl.key.password'],
            'ssl.truststore.location' : SecurityConfig.TRUSTSTORE_PATH,
            'ssl.truststore.password' : SecurityConfig.ssl_stores['ssl.truststore.password']
        }
    
    def setup_node(self, node):
        if self.security_protocol == SecurityConfig.SSL:
            node.account.ssh("mkdir -p %s" % SecurityConfig.SSL_DIR, allow_fail=False)
            node.account.scp_to(SecurityConfig.ssl_stores['ssl.keystore.location'], SecurityConfig.KEYSTORE_PATH)
            node.account.scp_to(SecurityConfig.ssl_stores['ssl.truststore.location'], SecurityConfig.TRUSTSTORE_PATH)

    def clean_node(self, node):
        if self.security_protocol == SecurityConfig.SSL:
            node.account.ssh("rm -rf %s" % SecurityConfig.SSL_DIR, allow_fail=False)

    def get_property(self, prop_name, template_props=""):
        """
        Get property value from the string representation of
        a properties file.
        """
        value = None
        for line in template_props.split("\n"):
            items = line.split("=")
            if len(items) == 2 and items[0].strip() == prop_name:
                value = str(items[1].strip())
        return value

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

