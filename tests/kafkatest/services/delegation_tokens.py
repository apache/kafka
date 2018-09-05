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

import os.path

"""
Delegation tokens is a tool to manage the lifecycle of delegation tokens.
All commands are executed on a secured Kafka node reusing its generated jaas.conf and krb5.conf.
"""

class DelegationTokens:
    def __init__(self, kafka):
        self.client_properties_content = """
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
-"""
        self.command_path = "/opt/kafka-dev/bin/kafka-delegation-tokens.sh"
        self.kafka_opts = "KAFKA_OPTS=\"-Djava.security.auth.login.config=/mnt/security/jaas.conf " \
                          "-Djava.security.krb5.conf=/mnt/security/krb5.conf\" "
        self.bootstrap_server = "  --bootstrap-server $(hostname -f):9094"
        self.base_cmd = self.kafka_opts + self.command_path + self.bootstrap_server
        self.kafka = kafka
        self.client_prop_path = os.path.join(self.kafka.PERSISTENT_ROOT, "client.properties")
        self.jaas_deleg_conf_path = os.path.join(self.kafka.PERSISTENT_ROOT, "jaas_deleg.conf")
        self.token_hmac_path = os.path.join(self.kafka.PERSISTENT_ROOT, "deleg_token_hmac.out")
        self.create_delegation_token_out = os.path.join(self.kafka.PERSISTENT_ROOT, "delegation_token.out")
        self.expire_delegation_token_out = os.path.join(self.kafka.PERSISTENT_ROOT, "expire_delegation_token.out")

        self.node = self.kafka.nodes[0]

    def generate_delegation_token(self):
        self.node.account.create_file(self.client_prop_path, self.client_properties_content)

        cmd = self.base_cmd + "  --create" \
                "  --max-life-time-period 1486750745585" \
                "  --command-config %s > %s" % (self.client_prop_path, self.create_delegation_token_out)
        self.node.account.ssh(cmd, allow_fail=False)

    def expire_delegation_token(self, hmac):
        cmd = self.base_cmd + "  --expire" \
                "  --expiry-time-period -1" \
                "  --hmac %s"  \
                "  --command-config %s > %s" % (hmac, self.client_prop_path, self.expire_delegation_token_out)
        self.node.account.ssh(cmd, allow_fail=False)

    def create_jaas_conf_with_delegation_token(self):
        jaas_deleg_content = """
KafkaClient {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="TOKEN_ID"
  password="TOKEN_HMAC"
  tokenauth=true;
};
"""
        self.node.account.create_file(self.jaas_deleg_conf_path, jaas_deleg_content)

        cmd = 'read -ra TOKEN <<< $(tail -1 %s) && ' \
              'echo -n ${TOKEN[1]} > %s && '\
              'sed -i -e "s/TOKEN_ID/${TOKEN[0]}/" %s &&' \
              'sed -i -e "s|TOKEN_HMAC|${TOKEN[1]}|" %s' \
              % (self.create_delegation_token_out,
                 self.token_hmac_path,
                 self.jaas_deleg_conf_path,
                 self.jaas_deleg_conf_path)

        self.node.account.ssh(cmd, allow_fail=False)

        with self.node.account.open(self.jaas_deleg_conf_path, "r") as f:
            jaas_deleg_conf = f.read()

        return jaas_deleg_conf

    def token_hmac(self):
        with self.node.account.open(self.token_hmac_path, "r") as f:
            token_hmac = f.read()
        return token_hmac