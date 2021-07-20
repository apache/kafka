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

from kafkatest.tests.core.replication_test \
    import ReplicationTest, clean_shutdown, matrix, parametrize, cluster, failures, quorum
from kafkatest.services.hadoop import MiniHadoop
from kafkatest.services.kafka import config_property


class TieredReplicationTest(ReplicationTest):

    TOPIC_CONFIG = {
        "partitions": 3,
        "replication-factor": 3,
        "configs": {"min.insync.replicas": 2, "remote.storage.enable": True}
    }

    def __init__(self, test_context):
        super(TieredReplicationTest, self).__init__(test_context)

    @cluster(num_nodes=10)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader"],
            security_protocol=["PLAINTEXT"],
            enable_idempotence=[True])
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader"],
            security_protocol=["PLAINTEXT", "SASL_SSL"],
            metadata_quorum=quorum.all_non_upgrade)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["controller"],
            security_protocol=["PLAINTEXT", "SASL_SSL"])
    @matrix(failure_mode=["hard_bounce"],
            broker_type=["leader"],
            security_protocol=["SASL_SSL"], client_sasl_mechanism=["PLAIN"], interbroker_sasl_mechanism=["PLAIN", "GSSAPI"],
            metadata_quorum=quorum.all_non_upgrade)
    @parametrize(failure_mode="hard_bounce",
                 broker_type="leader",
                 security_protocol="SASL_SSL", client_sasl_mechanism="SCRAM-SHA-256", interbroker_sasl_mechanism="SCRAM-SHA-512")
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            security_protocol=["PLAINTEXT"], broker_type=["leader"], compression_type=["gzip"], tls_version=["TLSv1.2", "TLSv1.3"],
            metadata_quorum=quorum.all_non_upgrade)
    def test_replication_with_broker_failure(self, failure_mode, security_protocol, broker_type,
                                             client_sasl_mechanism="GSSAPI", interbroker_sasl_mechanism="GSSAPI",
                                             compression_type=None, enable_idempotence=False, tls_version=None,
                                             metadata_quorum=quorum.zk):
        """Replication tests.
        These tests verify that replication provides simple durability guarantees by checking that data acked by
        brokers is still available for consumption in the face of various failure scenarios.

        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3, replication-factor=3, and min.insync.replicas=2

            - Produce messages in the background
            - Consume messages in the background
            - Drive broker failures (shutdown, or bounce repeatedly with kill -15 or kill -9)
            - When done driving failures, stop producing, and finish consuming
            - Validate that every acked message was consumed
        """
        self.hadoop = MiniHadoop(self.test_context, num_nodes=3)
        self.hadoop.start()

        if failure_mode == "controller" and metadata_quorum != quorum.zk:
            raise Exception("There is no controller broker when using a Raft-based metadata quorum")
        self.create_zookeeper_if_necessary()
        if self.zk:
            self.zk.start()

        server_prop_overides = [
            [config_property.REMOTE_LOG_STORAGE_SYSTEM_ENABLE, "true"],
            [config_property.REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME, "org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager"],
            [config_property.REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH,
             "/mnt/hadoop/config/etc/hadoop:/opt/kafka-dev/remote-storage-managers/hdfs/build/libs/*"
             + ":/opt/kafka-dev/remote-storage-managers/hdfs/build/dependant-libs/*"],
            [config_property.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME, security_protocol],

            [config_property.REMOTE_LOG_METADATA_MANAGER_IMPL_PREFIX, config_property.REMOTE_LOG_METADATA_MANAGER_PREFIX_CONFIG]
            [config_property.REMOTE_LOG_METADATA_TOPIC_NUM_PARTITIONS, "5"],

            [config_property.REMOTE_LOG_STORAGE_MANAGER_IMPL_PREFIX, config_property.REMOTE_LOG_STORAGE_MANAGER_PREFIX_CONFIG],
            [config_property.HDFS_BASE_DIR, "/test"],
            [config_property.HDFS_REMOTE_READ_CACHE_BYTES, "8388608"],
            [config_property.HDFS_REMOTE_READ_BYTES, "1048576"],
            # the `sasl.mechanism` is used by the kafka clients which gets invoked by the metadata manager.
            [config_property.SASL_MECHANISM, client_sasl_mechanism]
        ]
        self.create_kafka(num_nodes=3,
                          security_protocol=security_protocol,
                          interbroker_security_protocol=security_protocol,
                          client_sasl_mechanism=client_sasl_mechanism,
                          interbroker_sasl_mechanism=interbroker_sasl_mechanism,
                          tls_version=tls_version,
                          controller_num_nodes_override = 1,
                          server_prop_overides=server_prop_overides)
        self.kafka.start()

        compression_types = None if not compression_type else [compression_type]
        self.create_producer(compression_types=compression_types, enable_idempotence=enable_idempotence)
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.await_startup()
        failures[failure_mode](self, broker_type)
        self.run_validation(enable_idempotence=enable_idempotence)