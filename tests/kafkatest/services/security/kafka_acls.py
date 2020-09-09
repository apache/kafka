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

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin


class ACLs(KafkaPathResolverMixin):
    def __init__(self, context):
        self.context = context

    def set_acls(self, protocol, kafka, topic, group, force_use_zk_connection=False):
        # Set server ACLs
        kafka_principal = "User:CN=systemtest" if protocol == "SSL" else "User:kafka"
        kafka.add_cluster_acl(kafka_principal, force_use_zk_connection=force_use_zk_connection)
        kafka.add_read_acl(kafka_principal, "*", force_use_zk_connection=force_use_zk_connection)

        # Set client ACLs
        client_principal = "User:CN=systemtest" if protocol == "SSL" else "User:client"
        kafka.add_produce_acl(client_principal, topic, force_use_zk_connection=force_use_zk_connection)
        kafka.add_consume_acl(client_principal, topic, group, force_use_zk_connection=force_use_zk_connection)
