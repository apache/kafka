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

import atexit
import os
import subprocess
from tempfile import mkdtemp
from shutil import rmtree
from ducktape.template import TemplateRenderer
from kafkatest.services.security.minikdc import MiniKdc
import itertools

class SslStores(object):
    def __init__(self):
        self.ca_and_truststore_dir = mkdtemp(dir="/tmp")
        self.ca_crt_path = os.path.join(self.ca_and_truststore_dir, "test.ca.crt")
        self.ca_jks_path = os.path.join(self.ca_and_truststore_dir, "test.ca.jks")
        self.ca_passwd = "test-ca-passwd"

        self.truststore_path = os.path.join(self.ca_and_truststore_dir, "test.truststore.jks")
        self.truststore_passwd = "test-ts-passwd"
        self.keystore_passwd = "test-ks-passwd"
        self.key_passwd = "test-key-passwd"
        # Allow upto one hour of clock skew between host and VMs
        self.startdate = "-1H"
        # Register rmtree to run on exit
        atexit.register(rmtree, self.ca_and_truststore_dir)

    def generate_ca(self):
        """
        Generate CA private key and certificate.
        """

        self.runcmd("keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 -keystore %s -storetype JKS -storepass %s -keypass %s -dname CN=SystemTestCA -startdate %s" % (self.ca_jks_path, self.ca_passwd, self.ca_passwd, self.startdate))
        self.runcmd("keytool -export -alias ca -keystore %s -storepass %s -storetype JKS -rfc -file %s" % (self.ca_jks_path, self.ca_passwd, self.ca_crt_path))

    def generate_truststore(self):
        """
        Generate JKS truststore containing CA certificate.
        """

        self.runcmd("keytool -importcert -alias ca -file %s -keystore %s -storepass %s -storetype JKS -noprompt" % (self.ca_crt_path, self.truststore_path, self.truststore_passwd))

    def generate_and_copy_keystore(self, node):
        """
        Generate JKS keystore with certificate signed by the test CA.
        The generated certificate has the node's hostname as a DNS SubjectAlternativeName.
        """

        ks_dir = mkdtemp(dir="/tmp")
        ks_path = os.path.join(ks_dir, "test.keystore.jks")
        csr_path = os.path.join(ks_dir, "test.kafka.csr")
        crt_path = os.path.join(ks_dir, "test.kafka.crt")

        self.runcmd("keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -keystore %s -storepass %s -keypass %s -dname CN=systemtest -ext SAN=DNS:%s -startdate %s" % (ks_path, self.keystore_passwd, self.key_passwd, self.hostname(node), self.startdate))
        self.runcmd("keytool -certreq -keystore %s -storepass %s -keypass %s -alias kafka -file %s" % (ks_path, self.keystore_passwd, self.key_passwd, csr_path))
        self.runcmd("keytool -gencert -keystore %s -storepass %s -alias ca -infile %s -outfile %s -dname CN=systemtest -ext SAN=DNS:%s -startdate %s" % (self.ca_jks_path, self.ca_passwd, csr_path, crt_path, self.hostname(node), self.startdate))
        self.runcmd("keytool -importcert -keystore %s -storepass %s -alias ca -file %s -noprompt" % (ks_path, self.keystore_passwd, self.ca_crt_path))
        self.runcmd("keytool -importcert -keystore %s -storepass %s -keypass %s -alias kafka -file %s -noprompt" % (ks_path, self.keystore_passwd, self.key_passwd, crt_path))
        node.account.scp_to(ks_path, SecurityConfig.KEYSTORE_PATH)
        rmtree(ks_dir)

    def hostname(self, node):
        """ Hostname which may be overridden for testing validation failures
        """
        return node.account.hostname

    def runcmd(self, cmd):
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


class SecurityConfig(TemplateRenderer):

    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'
    SASL_MECHANISM_GSSAPI = 'GSSAPI'
    SASL_MECHANISM_PLAIN = 'PLAIN'
    CONFIG_DIR = "/mnt/security"
    KEYSTORE_PATH = "/mnt/security/test.keystore.jks"
    TRUSTSTORE_PATH = "/mnt/security/test.truststore.jks"
    JAAS_CONF_PATH = "/mnt/security/jaas.conf"
    KRB5CONF_PATH = "/mnt/security/krb5.conf"
    KEYTAB_PATH = "/mnt/security/keytab"

    ssl_stores = SslStores()
    ssl_stores.generate_ca()
    ssl_stores.generate_truststore()

    def __init__(self, security_protocol=None, interbroker_security_protocol=None,
                 client_sasl_mechanism=SASL_MECHANISM_GSSAPI, interbroker_sasl_mechanism=SASL_MECHANISM_GSSAPI,
                 zk_sasl=False, template_props=""):
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
        elif security_protocol not in [SecurityConfig.PLAINTEXT, SecurityConfig.SSL, SecurityConfig.SASL_PLAINTEXT, SecurityConfig.SASL_SSL]:
            raise Exception("Invalid security.protocol in template properties: " + security_protocol)

        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.interbroker_security_protocol = interbroker_security_protocol
        self.has_sasl = self.is_sasl(security_protocol) or self.is_sasl(interbroker_security_protocol) or zk_sasl
        self.has_ssl = self.is_ssl(security_protocol) or self.is_ssl(interbroker_security_protocol)
        self.zk_sasl = zk_sasl
        self.properties = {
            'security.protocol' : security_protocol,
            'ssl.keystore.location' : SecurityConfig.KEYSTORE_PATH,
            'ssl.keystore.password' : SecurityConfig.ssl_stores.keystore_passwd,
            'ssl.key.password' : SecurityConfig.ssl_stores.key_passwd,
            'ssl.truststore.location' : SecurityConfig.TRUSTSTORE_PATH,
            'ssl.truststore.password' : SecurityConfig.ssl_stores.truststore_passwd,
            'ssl.endpoint.identification.algorithm' : 'HTTPS',
            'sasl.mechanism' : client_sasl_mechanism,
            'sasl.mechanism.inter.broker.protocol' : interbroker_sasl_mechanism,
            'sasl.kerberos.service.name' : 'kafka'
        }


    def client_config(self, template_props=""):
        return SecurityConfig(self.security_protocol, client_sasl_mechanism=self.client_sasl_mechanism, template_props=template_props)

    def setup_ssl(self, node):
        node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
        node.account.scp_to(SecurityConfig.ssl_stores.truststore_path, SecurityConfig.TRUSTSTORE_PATH)
        SecurityConfig.ssl_stores.generate_and_copy_keystore(node)

    def setup_sasl(self, node):
        node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
        jaas_conf_file = "jaas.conf"
        java_version = node.account.ssh_capture("java -version")
        if any('IBM' in line for line in java_version):
            is_ibm_jdk = True
        else:
            is_ibm_jdk = False
        jaas_conf = self.render(jaas_conf_file,  node=node, is_ibm_jdk=is_ibm_jdk,
                                client_sasl_mechanism=self.client_sasl_mechanism,
                                enabled_sasl_mechanisms=self.enabled_sasl_mechanisms)
        node.account.create_file(SecurityConfig.JAAS_CONF_PATH, jaas_conf)
        if self.has_sasl_kerberos:
            node.account.scp_to(MiniKdc.LOCAL_KEYTAB_FILE, SecurityConfig.KEYTAB_PATH)
            node.account.scp_to(MiniKdc.LOCAL_KRB5CONF_FILE, SecurityConfig.KRB5CONF_PATH)

    def setup_node(self, node):
        if self.has_ssl:
            self.setup_ssl(node)

        if self.has_sasl:
            self.setup_sasl(node)

    def clean_node(self, node):
        if self.security_protocol != SecurityConfig.PLAINTEXT:
            node.account.ssh("rm -rf %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)

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

    def is_ssl(self, security_protocol):
        return security_protocol == SecurityConfig.SSL or security_protocol == SecurityConfig.SASL_SSL

    def is_sasl(self, security_protocol):
        return security_protocol == SecurityConfig.SASL_PLAINTEXT or security_protocol == SecurityConfig.SASL_SSL

    @property
    def security_protocol(self):
        return self.properties['security.protocol']

    @property
    def client_sasl_mechanism(self):
        return self.properties['sasl.mechanism']

    @property
    def interbroker_sasl_mechanism(self):
        return self.properties['sasl.mechanism.inter.broker.protocol']

    @property
    def enabled_sasl_mechanisms(self):
        return set([self.client_sasl_mechanism, self.interbroker_sasl_mechanism])

    @property
    def has_sasl_kerberos(self):
        return self.has_sasl and (SecurityConfig.SASL_MECHANISM_GSSAPI in self.enabled_sasl_mechanisms)

    @property
    def kafka_opts(self):
        if self.has_sasl:
            return "\"-Djava.security.auth.login.config=%s -Djava.security.krb5.conf=%s\"" % (SecurityConfig.JAAS_CONF_PATH, SecurityConfig.KRB5CONF_PATH)
        else:
            return ""

    def props(self, prefix=''):
        """
        Return properties as string with line separators, optionally with a prefix.
        This is used to append security config properties to
        a properties file.
        :param prefix: prefix to add to each property
        :return: a string containing line-separated properties
        """
        if self.security_protocol == SecurityConfig.PLAINTEXT:
            return ""
        config_lines = (prefix + key + "=" + value for key, value in self.properties.iteritems())
        # Extra blank lines ensure this can be appended/prepended safely
        return "\n".join(itertools.chain([""], config_lines, [""]))

    def __str__(self):
        """
        Return properties as a string with line separators.
        """
        return self.props()
