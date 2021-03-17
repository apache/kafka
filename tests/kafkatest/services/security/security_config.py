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

import logging
import os
import subprocess
from tempfile import mkdtemp
from shutil import rmtree
from ducktape.template import TemplateRenderer

from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.services.security.listener_security_config import ListenerSecurityConfig
import itertools

from kafkatest.utils.remote_account import java_version


class SslStores(object):
    def __init__(self, local_scratch_dir, logger=None):
        self.logger = logger
        self.ca_crt_path = os.path.join(local_scratch_dir, "test.ca.crt")
        self.ca_jks_path = os.path.join(local_scratch_dir, "test.ca.jks")
        self.ca_passwd = "test-ca-passwd"

        self.truststore_path = os.path.join(local_scratch_dir, "test.truststore.jks")
        self.truststore_passwd = "test-ts-passwd"
        self.keystore_passwd = "test-ks-passwd"
        # Zookeeper TLS (as of v3.5.6) does not support a key password different than the keystore password
        self.key_passwd = self.keystore_passwd
        # Allow upto one hour of clock skew between host and VMs
        self.startdate = "-1H"

        for file in [self.ca_crt_path, self.ca_jks_path, self.truststore_path]:
            if os.path.exists(file):
                os.remove(file)

    def generate_ca(self):
        """
        Generate CA private key and certificate.
        """

        self.runcmd("keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 -keystore %s -storetype JKS -storepass %s -keypass %s -dname CN=SystemTestCA -startdate %s --ext bc=ca:true" % (self.ca_jks_path, self.ca_passwd, self.ca_passwd, self.startdate))
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

        self.runcmd("keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -keystore %s -storepass %s -storetype JKS -keypass %s -dname CN=systemtest -ext SAN=DNS:%s -startdate %s" % (ks_path, self.keystore_passwd, self.key_passwd, self.hostname(node), self.startdate))
        self.runcmd("keytool -certreq -keystore %s -storepass %s -storetype JKS -keypass %s -alias kafka -file %s" % (ks_path, self.keystore_passwd, self.key_passwd, csr_path))
        self.runcmd("keytool -gencert -keystore %s -storepass %s -storetype JKS -alias ca -infile %s -outfile %s -dname CN=systemtest -ext SAN=DNS:%s -startdate %s" % (self.ca_jks_path, self.ca_passwd, csr_path, crt_path, self.hostname(node), self.startdate))
        self.runcmd("keytool -importcert -keystore %s -storepass %s -storetype JKS -alias ca -file %s -noprompt" % (ks_path, self.keystore_passwd, self.ca_crt_path))
        self.runcmd("keytool -importcert -keystore %s -storepass %s -storetype JKS -keypass %s -alias kafka -file %s -noprompt" % (ks_path, self.keystore_passwd, self.key_passwd, crt_path))
        node.account.copy_to(ks_path, SecurityConfig.KEYSTORE_PATH)

        # generate ZooKeeper client TLS config file for encryption-only (no client cert) use case
        str = """zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
zookeeper.ssl.client.enable=true
zookeeper.ssl.truststore.location=%s
zookeeper.ssl.truststore.password=%s
""" % (SecurityConfig.TRUSTSTORE_PATH, self.truststore_passwd)
        node.account.create_file(SecurityConfig.ZK_CLIENT_TLS_ENCRYPT_ONLY_CONFIG_PATH, str)

        # also generate ZooKeeper client TLS config file for mutual authentication use case
        str = """zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
zookeeper.ssl.client.enable=true
zookeeper.ssl.truststore.location=%s
zookeeper.ssl.truststore.password=%s
zookeeper.ssl.keystore.location=%s
zookeeper.ssl.keystore.password=%s
""" % (SecurityConfig.TRUSTSTORE_PATH, self.truststore_passwd, SecurityConfig.KEYSTORE_PATH, self.keystore_passwd)
        node.account.create_file(SecurityConfig.ZK_CLIENT_MUTUAL_AUTH_CONFIG_PATH, str)

        rmtree(ks_dir)

    def hostname(self, node):
        """ Hostname which may be overridden for testing validation failures
        """
        return node.account.hostname

    def runcmd(self, cmd):
        if self.logger:
            self.logger.log(logging.DEBUG, cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError("Command '%s' returned non-zero exit status %d: %s" % (cmd, proc.returncode, stdout))


class SecurityConfig(TemplateRenderer):

    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'
    SASL_SECURITY_PROTOCOLS = [SASL_PLAINTEXT, SASL_SSL]
    SSL_SECURITY_PROTOCOLS = [SSL, SASL_SSL]
    SASL_MECHANISM_GSSAPI = 'GSSAPI'
    SASL_MECHANISM_PLAIN = 'PLAIN'
    SASL_MECHANISM_SCRAM_SHA_256 = 'SCRAM-SHA-256'
    SASL_MECHANISM_SCRAM_SHA_512 = 'SCRAM-SHA-512'
    SCRAM_CLIENT_USER = "kafka-client"
    SCRAM_CLIENT_PASSWORD = "client-secret"
    SCRAM_BROKER_USER = "kafka-broker"
    SCRAM_BROKER_PASSWORD = "broker-secret"
    CONFIG_DIR = "/mnt/security"
    KEYSTORE_PATH = "/mnt/security/test.keystore.jks"
    TRUSTSTORE_PATH = "/mnt/security/test.truststore.jks"
    ZK_CLIENT_TLS_ENCRYPT_ONLY_CONFIG_PATH = "/mnt/security/zk_client_tls_encrypt_only_config.properties"
    ZK_CLIENT_MUTUAL_AUTH_CONFIG_PATH = "/mnt/security/zk_client_mutual_auth_config.properties"
    JAAS_CONF_PATH = "/mnt/security/jaas.conf"
    # allows admin client to connect with broker credentials to create User SCRAM credentials
    ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PATH = "/mnt/security/admin_client_as_broker_jaas.conf"
    KRB5CONF_PATH = "/mnt/security/krb5.conf"
    KEYTAB_PATH = "/mnt/security/keytab"

    # This is initialized only when the first instance of SecurityConfig is created
    ssl_stores = None

    def __init__(self, context, security_protocol=None, interbroker_security_protocol=None,
                 client_sasl_mechanism=SASL_MECHANISM_GSSAPI, interbroker_sasl_mechanism=SASL_MECHANISM_GSSAPI,
                 zk_sasl=False, zk_tls=False, template_props="", static_jaas_conf=True, jaas_override_variables=None,
                 listener_security_config=ListenerSecurityConfig(), tls_version=None,
                 serves_controller_sasl_mechanism=None, # Raft Controller does this
                 serves_intercontroller_sasl_mechanism=None, # Raft Controller does this
                 uses_controller_sasl_mechanism=None, # communication to Raft Controller (broker and controller both do this)
                 raft_tls=False):
        """
        Initialize the security properties for the node and copy
        keystore and truststore to the remote node if the transport protocol 
        is SSL. If security_protocol is None, the protocol specified in the
        template properties file is used. If no protocol is specified in the
        template properties either, PLAINTEXT is used as default.
        """

        self.context = context
        if not SecurityConfig.ssl_stores:
            # This generates keystore/trustore files in a local scratch directory which gets
            # automatically destroyed after the test is run
            # Creating within the scratch directory allows us to run tests in parallel without fear of collision
            SecurityConfig.ssl_stores = SslStores(context.local_scratch_dir, context.logger)
            SecurityConfig.ssl_stores.generate_ca()
            SecurityConfig.ssl_stores.generate_truststore()

        if security_protocol is None:
            security_protocol = self.get_property('security.protocol', template_props)
        if security_protocol is None:
            security_protocol = SecurityConfig.PLAINTEXT
        elif security_protocol not in [SecurityConfig.PLAINTEXT, SecurityConfig.SSL, SecurityConfig.SASL_PLAINTEXT, SecurityConfig.SASL_SSL]:
            raise Exception("Invalid security.protocol in template properties: " + security_protocol)

        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.interbroker_security_protocol = interbroker_security_protocol
        serves_raft_sasl = []
        if serves_controller_sasl_mechanism is not None:
            serves_raft_sasl += [serves_controller_sasl_mechanism]
        if serves_intercontroller_sasl_mechanism is not None:
            serves_raft_sasl += [serves_intercontroller_sasl_mechanism]
        self.serves_raft_sasl = set(serves_raft_sasl)
        uses_raft_sasl = []
        if uses_controller_sasl_mechanism is not None:
            uses_raft_sasl += [uses_controller_sasl_mechanism]
        self.uses_raft_sasl = set(uses_raft_sasl)

        self.zk_sasl = zk_sasl
        self.zk_tls = zk_tls
        self.static_jaas_conf = static_jaas_conf
        self.listener_security_config = listener_security_config
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
        self.raft_tls = raft_tls

        if tls_version is not None:
            self.properties.update({'tls.version' : tls_version})

        self.properties.update(self.listener_security_config.client_listener_overrides)
        self.jaas_override_variables = jaas_override_variables or {}

        self.calc_has_sasl()
        self.calc_has_ssl()

    def calc_has_sasl(self):
        self.has_sasl = self.is_sasl(self.properties['security.protocol']) \
                        or self.is_sasl(self.interbroker_security_protocol) \
                        or self.zk_sasl \
                        or self.serves_raft_sasl or self.uses_raft_sasl

    def calc_has_ssl(self):
        self.has_ssl = self.is_ssl(self.properties['security.protocol']) \
                       or self.is_ssl(self.interbroker_security_protocol) \
                       or self.zk_tls \
                       or self.raft_tls

    def client_config(self, template_props="", node=None, jaas_override_variables=None,
                      use_inter_broker_mechanism_for_client = False):
        # If node is not specified, use static jaas config which will be created later.
        # Otherwise use static JAAS configuration files with SASL_SSL and sasl.jaas.config
        # property with SASL_PLAINTEXT so that both code paths are tested by existing tests.
        # Note that this is an arbitrary choice and it is possible to run all tests with
        # either static or dynamic jaas config files if required.
        static_jaas_conf = node is None or (self.has_sasl and self.has_ssl)
        if use_inter_broker_mechanism_for_client:
            client_sasl_mechanism_to_use = self.interbroker_sasl_mechanism
        else:
            # csv is supported here, but client configs only supports a single mechanism,
            # so arbitrarily take the first one defined in case it has multiple values
            client_sasl_mechanism_to_use = self.client_sasl_mechanism.split(',')[0].strip()

        return SecurityConfig(self.context, self.security_protocol,
                              client_sasl_mechanism=client_sasl_mechanism_to_use,
                              template_props=template_props,
                              static_jaas_conf=static_jaas_conf,
                              jaas_override_variables=jaas_override_variables,
                              listener_security_config=self.listener_security_config,
                              tls_version=self.tls_version)

    def enable_sasl(self):
        self.has_sasl = True

    def enable_ssl(self):
        self.has_ssl = True

    def enable_security_protocol(self, security_protocol):
        self.has_sasl = self.has_sasl or self.is_sasl(security_protocol)
        self.has_ssl = self.has_ssl or self.is_ssl(security_protocol)

    def setup_ssl(self, node):
        node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
        node.account.copy_to(SecurityConfig.ssl_stores.truststore_path, SecurityConfig.TRUSTSTORE_PATH)
        SecurityConfig.ssl_stores.generate_and_copy_keystore(node)

    def setup_sasl(self, node):
        node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
        jaas_conf_file = "jaas.conf"
        java_version = node.account.ssh_capture("java -version")

        jaas_conf = None
        if 'sasl.jaas.config' not in self.properties:
            jaas_conf = self.render_jaas_config(
                jaas_conf_file,
                {
                    'node': node,
                    'is_ibm_jdk': any('IBM' in line for line in java_version),
                    'SecurityConfig': SecurityConfig,
                    'client_sasl_mechanism': self.client_sasl_mechanism,
                    'enabled_sasl_mechanisms': self.enabled_sasl_mechanisms
                }
            )
        else:
            jaas_conf = self.properties['sasl.jaas.config']

        if self.static_jaas_conf:
            node.account.create_file(SecurityConfig.JAAS_CONF_PATH, jaas_conf)
            node.account.create_file(SecurityConfig.ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PATH,
                                     self.render_jaas_config(
                                         "admin_client_as_broker_jaas.conf",
                                         {
                                             'node': node,
                                             'is_ibm_jdk': any('IBM' in line for line in java_version),
                                             'SecurityConfig': SecurityConfig,
                                             'client_sasl_mechanism': self.client_sasl_mechanism,
                                             'enabled_sasl_mechanisms': self.enabled_sasl_mechanisms
                                         }
                                     ))

        elif 'sasl.jaas.config' not in self.properties:
            self.properties['sasl.jaas.config'] = jaas_conf.replace("\n", " \\\n")
        if self.has_sasl_kerberos:
            node.account.copy_to(MiniKdc.LOCAL_KEYTAB_FILE, SecurityConfig.KEYTAB_PATH)
            node.account.copy_to(MiniKdc.LOCAL_KRB5CONF_FILE, SecurityConfig.KRB5CONF_PATH)

    def render_jaas_config(self, jaas_conf_file, config_variables):
        """
        Renders the JAAS config file contents

        :param jaas_conf_file: name of the JAAS config template file
        :param config_variables: dict of variables used in the template
        :return: the rendered template string
        """
        variables = config_variables.copy()
        variables.update(self.jaas_override_variables)  # override variables
        return self.render(jaas_conf_file, **variables)

    def setup_node(self, node):
        if self.has_ssl:
            self.setup_ssl(node)

        if self.has_sasl:
            self.setup_sasl(node)

        if java_version(node) <= 11 and self.properties.get('tls.version') == 'TLSv1.3':
            self.properties.update({'tls.version': 'TLSv1.2'})

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
        return security_protocol in SecurityConfig.SSL_SECURITY_PROTOCOLS

    def is_sasl(self, security_protocol):
        return security_protocol in SecurityConfig.SASL_SECURITY_PROTOCOLS

    def is_sasl_scram(self, sasl_mechanism):
        return sasl_mechanism == SecurityConfig.SASL_MECHANISM_SCRAM_SHA_256 or sasl_mechanism == SecurityConfig.SASL_MECHANISM_SCRAM_SHA_512

    @property
    def security_protocol(self):
        return self.properties['security.protocol']

    @property
    def tls_version(self):
        return self.properties.get('tls.version')

    @property
    def client_sasl_mechanism(self):
        return self.properties['sasl.mechanism']

    @property
    def interbroker_sasl_mechanism(self):
        return self.properties['sasl.mechanism.inter.broker.protocol']

    @property
    def enabled_sasl_mechanisms(self):
        """
        :return: all the SASL mechanisms in use, including for brokers, clients, controllers, and ZooKeeper
        """
        sasl_mechanisms = []
        if self.is_sasl(self.security_protocol):
            # .csv is supported so be sure to account for that possibility
            sasl_mechanisms += [mechanism.strip() for mechanism in self.client_sasl_mechanism.split(',')]
        if self.is_sasl(self.interbroker_security_protocol):
            sasl_mechanisms += [self.interbroker_sasl_mechanism]
        if self.serves_raft_sasl:
            sasl_mechanisms += list(self.serves_raft_sasl)
        if self.uses_raft_sasl:
            sasl_mechanisms += list(self.uses_raft_sasl)
        if self.zk_sasl:
            sasl_mechanisms += [SecurityConfig.SASL_MECHANISM_GSSAPI]
        return set(sasl_mechanisms)

    @property
    def has_sasl_kerberos(self):
        return self.has_sasl and (SecurityConfig.SASL_MECHANISM_GSSAPI in self.enabled_sasl_mechanisms)

    @property
    def kafka_opts(self):
        if self.has_sasl:
            if self.static_jaas_conf:
                return "\"-Djava.security.auth.login.config=%s -Djava.security.krb5.conf=%s\"" % (SecurityConfig.JAAS_CONF_PATH, SecurityConfig.KRB5CONF_PATH)
            else:
                return "\"-Djava.security.krb5.conf=%s\"" % SecurityConfig.KRB5CONF_PATH
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
        if self.has_sasl and not self.static_jaas_conf and 'sasl.jaas.config' not in self.properties:
            raise Exception("JAAS configuration property has not yet been initialized")
        config_lines = (prefix + key + "=" + value for key, value in self.properties.items())
        # Extra blank lines ensure this can be appended/prepended safely
        return "\n".join(itertools.chain([""], config_lines, [""]))

    def __str__(self):
        """
        Return properties as a string with line separators.
        """
        return self.props()
