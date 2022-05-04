from ducktape.tests.test import Test
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService


class MiniTest(Test):
    def __init__(self, test_context):
        super(MiniTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, 1)
        self.kafka = KafkaService(test_context, 1, self.zk)

    @cluster(num_nodes=2)
    def test(self):
        self.zk.start()
        self.kafka.start()
