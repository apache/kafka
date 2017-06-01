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

from ducktape.tests.test import Test
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.console_consumer import ConsoleConsumer

class QuotaConfig(object):

    USER_PRINCIPAL = 'CN=systemtest'
    THROTTLED_ID = 'throttled_client_id'
    UNTHROTTLED_ID = 'unthrottled_client_id'

    SMALL_REQUEST_PERCENT = 1.0
    LARGE_REQUEST_PERCENT = 5000

    def __init__(self, kafka):
        self.kafka = kafka


    def configure_quota(self, request_percentage, user, client_id):
        node = self.kafka.nodes[0]
        cmd = "%s --zookeeper %s --alter --add-config request_percentage=%f" % \
              (self.kafka.path.script("kafka-configs.sh", node), self.kafka.zk.connect_setting(), request_percentage)
        if user is not None:
            cmd += " --entity-type users --entity-name %s" % user
        if client_id is not None:
            cmd += " --entity-type clients --entity-name %s" % client_id
        node.account.ssh(cmd)


class RequestQuotaTest(Test):
    """
    Tests to verify that clients with small request quota are throttled,
    clients with large request quota are not throttled and broker's
    exempt metrics as well as client throttle netrics are updated.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(RequestQuotaTest, self).__init__(test_context=test_context)

        self.topic = 'test_topic'
        self.logger.info('use topic ' + self.topic)

        self.num_records = 50000
        self.record_size = 3000

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  security_protocol='SSL', authorizer_class_name='',
                                  interbroker_security_protocol='SSL',
                                  topics={self.topic: {'partitions': 6, 'replication-factor': 1, 'configs': {'min.insync.replicas': 1}}},
                                  jmx_object_names=['kafka.server:type=Request'],
                                  jmx_attributes=['exempt-request-time'])

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    @cluster(num_nodes=7)
    def test_request_quota(self):
        quota_config = QuotaConfig(self.kafka)
        throttled_client_id = QuotaConfig.THROTTLED_ID
        unthrottled_client_id = QuotaConfig.UNTHROTTLED_ID

        # Configure small quota for throttled_client_id
        quota_config.configure_quota(QuotaConfig.SMALL_REQUEST_PERCENT, QuotaConfig.USER_PRINCIPAL, throttled_client_id)
        # Configure large defaults for other clients of user
        quota_config.configure_quota(QuotaConfig.LARGE_REQUEST_PERCENT, QuotaConfig.USER_PRINCIPAL, None)

        throttled_consumer = self.createConsumer(throttled_client_id)
        throttled_consumer.start()
        throttled_consumer.wait_for_init(10)
        throttled_producer = self.createProducer(throttled_client_id)
        throttled_producer.run()
        throttled_consumer.wait()

        unthrottled_consumer = self.createConsumer(unthrottled_client_id)
        unthrottled_consumer.start()
        unthrottled_consumer.wait_for_init(10)
        unthrottled_producer = self.createProducer(unthrottled_client_id)
        unthrottled_producer.run()
        unthrottled_consumer.wait()

        self.kafka.read_jmx_output_all_nodes()

        self.validate_request_quota(throttled_producer, throttled_consumer, unthrottled_producer, unthrottled_consumer)

    def createProducer(self, producer_client_id):
        return ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=self.topic, num_records=self.num_records, record_size=self.record_size, throughput=2000,
            client_id=producer_client_id,
            jmx_object_names=['kafka.producer:type=producer-metrics,client-id=%s' % producer_client_id],
            jmx_attributes=['outgoing-byte-rate,produce-throttle-time-avg,produce-throttle-time-max'])

    def createConsumer(self, consumer_client_id):
        return ConsoleConsumer(self.test_context, 1, self.kafka, self.topic,
             consumer_timeout_ms=5000, client_id=consumer_client_id, fetch_max_wait_ms=0,
             jmx_object_names=['kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s' % consumer_client_id],
             jmx_attributes=['bytes-consumed-rate,fetch-throttle-time-avg,fetch-throttle-time-max'])

    def max_produce_metric(self, producer, metric_name):
        attribute_name = 'kafka.producer:type=producer-metrics,client-id=%s:%s' % (producer.client_id, metric_name)
        return producer.maximum_jmx_value[attribute_name]

    def max_consume_metric(self, consumer, metric_name):
        attribute_name = 'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s:%s' % (consumer.client_id, metric_name)
        return consumer.maximum_jmx_value[attribute_name]

    def avg_produce_metric(self, producer, metric_name):
        attribute_name = 'kafka.producer:type=producer-metrics,client-id=%s:%s' % (producer.client_id, metric_name)
        return producer.average_jmx_value[attribute_name]

    def avg_consume_metric(self, consumer, metric_name):
        attribute_name = 'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s:%s' % (consumer.client_id, metric_name)
        return consumer.average_jmx_value[attribute_name]

    def validate_request_quota(self, throttled_producer, throttled_consumer, unthrottled_producer, unthrottled_consumer):
        """
        Validate that:
        1) Clients with small request quota have been throttled
        2) Maximum throttle time is limited by the quota window size of 1 second
        3) Clients with large request quota are not throttled
        4) Clients with large request quota have higher byte rate than clients with small quota
        5) Broker's exempt quota metric has been updated
        """

        throttle_avg = self.avg_produce_metric(throttled_producer, 'produce-throttle-time-avg')
        throttle_max = self.max_produce_metric(throttled_producer, 'produce-throttle-time-max')
        self.logger.info("Producer throttle time avg=%.5f ms max=%.5f ms" % (throttle_avg, throttle_max))
        assert throttle_avg > 0.0, "Producer should have been throttled"
        assert throttle_max <= 1000.0, "Producer throttle time too high %.5f" % throttle_max

        throttle_avg = self.avg_consume_metric(throttled_consumer, 'fetch-throttle-time-avg')
        throttle_max = self.max_consume_metric(throttled_consumer, 'fetch-throttle-time-max')
        self.logger.info("Consumer throttle time avg=%.5f ms max=%.5f ms" % (throttle_avg, throttle_max))
        assert throttle_avg > 0.0, "Consumer should have been throttled"
        assert throttle_max <= 1000.0, "Consumer throttle time too high %.5f" % throttle_max

        throttle_max = self.max_produce_metric(unthrottled_producer, 'produce-throttle-time-max')
        assert throttle_max == 0.0, "Producer should not have been throttled, max=%.5f" % throttle_max
        throttle_max  = self.max_consume_metric(unthrottled_consumer, 'fetch-throttle-time-max')
        assert throttle_max == 0.0, "Consumer should not have been throttled, max=%.5f" % throttle_max

        throttled_rate = self.max_produce_metric(throttled_producer, 'outgoing-byte-rate')
        unthrottled_rate = self.max_produce_metric(unthrottled_producer, 'outgoing-byte-rate')
        assert throttled_rate < unthrottled_rate, \
            "Throttling had no impact on produce rate throttled_rate=%.5f unthrottled_rate=%.5f" % (throttled_rate, unthrottled_rate)

        throttled_rate = self.max_consume_metric(throttled_consumer, 'bytes-consumed-rate')
        unthrottled_rate = self.max_consume_metric(unthrottled_consumer, 'bytes-consumed-rate')
        assert throttled_rate < unthrottled_rate, \
            "Throttling had no impact on consume rate throttled_rate=%.5f unthrottled_rate=%.5f" % (throttled_rate, unthrottled_rate)

        broker_exempt_throttle_name = 'kafka.server:type=Request:exempt-request-time'
        exempt_throttle_avg = self.kafka.average_jmx_value[broker_exempt_throttle_name]
        exempt_throttle_max = self.kafka.maximum_jmx_value[broker_exempt_throttle_name]
        self.logger.info("Broker exempt throttle percent avg=%.5f max=%.5f" % (exempt_throttle_avg, exempt_throttle_max))
        assert exempt_throttle_avg > 0.0 and exempt_throttle_max > 0.0, \
            "Throttle exempt metrics not updated, exempt=%.5f" % exempt_throttle_max

