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
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin

"""
Delegation tokens is a tool to manage the lifecycle of delegation tokens.
All commands are executed on a secured Kafka node reusing its generated jaas.conf and krb5.conf.
"""

class DelegationTokens(KafkaPathResolverMixin):
    def __init__(self, kafka, context):
        self.client_properties_content = """
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
"""
        self.context = context
        self.command_path = self.path.script("kafka-delegation-tokens.sh")
        self.kafka_opts = "KAFKA_OPTS=\"-Djava.security.auth.login.config=/mnt/security/jaas.conf " \
                          "-Djava.security.krb5.conf=/mnt/security/krb5.conf\" "
        self.kafka = kafka
        self.bootstrap_server = " --bootstrap-server " + self.kafka.bootstrap_servers('SASL_PLAINTEXT')
        self.base_cmd = self.kafka_opts + self.command_path + self.bootstrap_server
        self.client_prop_path = os.path.join(self.kafka.PERSISTENT_ROOT, "client.properties")
        self.jaas_deleg_conf_path = os.path.join(self.kafka.PERSISTENT_ROOT, "jaas_deleg.conf")
        self.token_hmac_path = os.path.join(self.kafka.PERSISTENT_ROOT, "deleg_token_hmac.out")
        self.delegation_token_out = os.path.join(self.kafka.PERSISTENT_ROOT, "delegation_token.out")
        self.expire_delegation_token_out = os.path.join(self.kafka.PERSISTENT_ROOT, "expire_delegation_token.out")
        self.renew_delegation_token_out = os.path.join(self.kafka.PERSISTENT_ROOT, "renew_delegation_token.out")

        self.node = self.kafka.nodes[0]

    def generate_delegation_token(self, maxlifetimeperiod=-1):
        self.node.account.create_file(self.client_prop_path, self.client_properties_content)

        cmd = self.base_cmd + "  --create" \
                              "  --max-life-time-period %s" \
                              "  --command-config %s > %s" % (maxlifetimeperiod, self.client_prop_path, self.delegation_token_out)
        self.node.account.ssh(cmd, allow_fail=False)

    def expire_delegation_token(self, hmac):
        cmd = self.base_cmd + "  --expire" \
                              "  --expiry-time-period -1" \
                              "  --hmac %s" \
                              "  --command-config %s > %s" % (hmac, self.client_prop_path, self.expire_delegation_token_out)
        self.node.account.ssh(cmd, allow_fail=False)

    def renew_delegation_token(self, hmac, renew_time_period=-1):
        cmd = self.base_cmd + "  --renew" \
                              "  --renew-time-period %s" \
                              "  --hmac %s" \
                              "  --command-config %s > %s" \
              % (renew_time_period, hmac, self.client_prop_path, self.renew_delegation_token_out)
        return self.node.account.ssh_capture(cmd, allow_fail=False)

    def create_jaas_conf_with_delegation_token(self):
        dt = self.parse_delegation_token_out()
        jaas_deleg_content = """
KafkaClient {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="%s"
  password="%s"
  tokenauth=true;
};
""" % (dt["tokenid"], dt["hmac"])
        self.node.account.create_file(self.jaas_deleg_conf_path, jaas_deleg_content)

        return jaas_deleg_content

    def token_hmac(self):
        dt = self.parse_delegation_token_out()
        return dt["hmac"]

    def parse_delegation_token_out(self):
        cmd = "tail -1 %s" % self.delegation_token_out

        output_iter = self.node.account.ssh_capture(cmd, allow_fail=False)
        output = ""
        for line in output_iter:
            output += line

        parts = output.split()
        try:
            tokenid, hmac, owner, requester, renewers, issuedate, expirydate, maxdate = parts
        except ValueError:
            raise ValueError("Could not parse %s, got parts %s" % (output, parts))

        return {"tokenid" : tokenid,
                "hmac" : hmac,
                "owner" : owner,
                "requester": requester,
                "renewers" : renewers,
                "issuedate" : issuedate,
                "expirydate" :expirydate,
                "maxdate" : maxdate}