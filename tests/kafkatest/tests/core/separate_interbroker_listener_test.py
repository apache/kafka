import time

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class TestSeparateInterbrokerListener(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestSeparateInterbrokerListener, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = 'test_topic'
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            'configs': {"min.insync.replicas": 2}}})
        self.zk.start()

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, 1, self.kafka, self.topic, throughput=100)

        self.consumer = ConsoleConsumer(
            self.test_context, 1, self.kafka, self.topic, consumer_timeout_ms=60000, message_validator=is_int)

    def roll_in_interbroker_listener(self, broker_protocol, broker_sasl_mechanism, use_separate_listener=False):
        self.kafka.setup_interbroker_listener(broker_protocol, use_separate_listener)
        self.kafka.interbroker_sasl_mechanism = broker_sasl_mechanism
        self.bounce()

    def bounce(self):
        self.kafka.start_minikdc()
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            time.sleep(10)

    @cluster(num_nodes=9)
    @parametrize(broker_protocol=SecurityConfig.SASL_SSL, broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI)
    @parametrize(broker_protocol=SecurityConfig.SASL_SSL, broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN)
    @parametrize(broker_protocol=SecurityConfig.SASL_PLAINTEXT,
                 broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN)
    def test_enable_separate_interbroker_listener(self, broker_protocol, broker_sasl_mechanism):
        """
        Start with a cluster that has a single {{client_protocol}} listener.
        Start producer and consumer on the {{client_protocol}} listener.
        Open a SECURED dedicated interbroker port via rolling upgrade.
        Ensure we can produce and consume via {{client_protocol}} port throughout.
        """
        client_protocol = SecurityConfig.SASL_SSL
        client_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI

        self.kafka.security_protocol = client_protocol
        self.kafka.client_sasl_mechanism = client_sasl_mechanism
        self.kafka.setup_interbroker_listener(client_protocol, use_separate_listener=False)
        self.kafka.interbroker_sasl_mechanism = client_sasl_mechanism

        self.kafka.start()

        # create plaintext producer and consumer
        self.create_producer_and_consumer()

        # run produce/consume/validate loop while enabling a separate interbroker listener via rolling restart
        self.run_produce_consume_validate(
            self.roll_in_interbroker_listener, broker_protocol, broker_sasl_mechanism, True)

    @cluster(num_nodes=9)
    @parametrize(broker_protocol=SecurityConfig.SASL_SSL, broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI)
    @parametrize(broker_protocol=SecurityConfig.SASL_SSL, broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN)
    @parametrize(broker_protocol=SecurityConfig.SASL_PLAINTEXT,
                 broker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN)
    def test_disable_separate_interbroker_listener(self, broker_protocol, broker_sasl_mechanism):
        """
        Start with a cluster that has two listeners, one on {{client_protocol}}, another on {{broker_protocol}}.
        Even if protocols are the same, it's still two listeners, interbroker listener is a dedicated one.
        Start producer and consumer on {{client_protocol}} listener.
        Close dedicated {{broker_protocol}} listener via rolling restart.
        Ensure we can produce and consume via {{client_protocol}} listener throughout.
        """
        client_protocol = SecurityConfig.SASL_SSL
        client_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI

        self.kafka.security_protocol = client_protocol
        self.kafka.client_sasl_mechanism = client_sasl_mechanism
        self.kafka.setup_interbroker_listener(broker_protocol, use_separate_listener=True)
        self.kafka.interbroker_sasl_mechanism = broker_sasl_mechanism

        self.kafka.start()
        # create producer and consumer via client security protocol
        self.create_producer_and_consumer()

        # run produce/consume/validate loop while disabling a separate interbroker listener via rolling restart
        self.run_produce_consume_validate(
            self.roll_in_interbroker_listener, client_protocol, client_sasl_mechanism, False)
