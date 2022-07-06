from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum


class MiniTest(Test):
    def __init__(self, test_context):
        super().__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, 1) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(test_context, 1, self.zk)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=quorum.all)
    def test(self, metadata_quorum):
        # zk will be none if using kraft
        if self.zk:
            self.zk.start()
        self.kafka.start()
