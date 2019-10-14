import time
from ducktape.mark import matrix
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, \
    LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, DEV_BRANCH, DEV_VERSION, KafkaVersion
from kafkatest.services.streams import CooperativeRebalanceUpgradeService
from kafkatest.tests.streams.utils import verify_stopped, stop_processors, verify_running


class StreamsCooperativeRebalanceUpgradeTest(Test):
    """
    Test of a rolling upgrade from eager rebalance to
    cooperative rebalance
    """

    source_topic = "source"
    sink_topic = "sink"
    thread_delimiter = "&"
    task_delimiter = "#"
    report_interval = "100"
    processing_message = "Processed [0-9]* records so far"
    stopped_message = "COOPERATIVE-REBALANCE-TEST-CLIENT-CLOSED"
    running_state_msg = "STREAMS in a RUNNING State"
    cooperative_turned_off_msg = "Turning off cooperative rebalancing for upgrade from %s"
    cooperative_enabled_msg = "Cooperative rebalancing enabled now"

    #streams_eager_rebalance_upgrade_versions = [str(LATEST_0_10_0), str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0),
    #                                            str(LATEST_1_0), str(LATEST_1_1), str(LATEST_2_0), str(LATEST_2_1), str(LATEST_2_2),
    #                                            str(LATEST_2_3)]

    streams_eager_rebalance_upgrade_versions = [str(LATEST_2_3)]

    def __init__(self, test_context):
        super(StreamsCooperativeRebalanceUpgradeTest, self).__init__(test_context)
        self.topics = {
            self.source_topic: {'partitions': 9},
            self.sink_topic: {'partitions': 9}
        }

        self.zookeeper = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zookeeper, topics=self.topics)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.source_topic,
                                           throughput=1000,
                                           acks=1)

    @matrix(upgrade_from_version=streams_eager_rebalance_upgrade_versions)
    def test_upgrade_to_cooperative_rebalance(self, upgrade_from_version):
        self.zookeeper.start()
        self.kafka.start()

        processor1 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)
        processor2 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)
        processor3 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)

        processors = [processor1, processor2, processor3]

        # produce records continually during the test
        self.producer.start()

        # start all processors without upgrade_from config; normal operations mode
        for processor in processors:
            processor.set_version(upgrade_from_version)
            self.set_props(processor)
            processor.CLEAN_NODE_ENABLED = False
            self.verify_running(processor, self.running_state_msg)

        for processor in processors:
            self.verify_processing(processor, self.processing_message)

        stop_processors(processors, self.stopped_message)

        # start again first rolling bounce with upgrade_from conifg set
        for processor in processors:
            # upgrade to version with cooperative rebalance
            processor.set_version(str(DEV_VERSION))
            self.set_props(processor, upgrade_from_version)
            node = processor.node
            with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                processor.start()
                message = self.cooperative_turned_off_msg % upgrade_from_version[:upgrade_from_version.rfind('.')]
                # verify cooperative turned off
                monitor.wait_until(message,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % message + str(processor.node.account))

                # verify rebalanced into a running state
                monitor.wait_until(self.running_state_msg,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % self.running_state_msg + str(processor.node.account))

                # verify processing
                monitor.wait_until(self.processing_message,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % self.processing_message + str(processor.node.account))

        # stop all instances again to prepare for
        # another rolling bounce without upgrade from to enable cooperative rebalance
        stop_processors(processors, self.stopped_message)

        # start again second rolling bounce without upgrade_from conifg
        for processor in processors:
            # upgrade to version with cooperative rebalance
            processor.set_version(str(DEV_VERSION))
            # removes the upgrade_from config
            self.set_props(processor)
            node = processor.node
            with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                processor.start()
                # verify cooperative turned off
                monitor.wait_until(self.cooperative_enabled_msg,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % self.cooperative_enabled_msg + str(processor.node.account))

                # verify rebalanced into a running state
                monitor.wait_until(self.running_state_msg,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % self.running_state_msg + str(processor.node.account))

                # verify processing
                monitor.wait_until(self.processing_message,
                                   timeout_sec=60,
                                   err_msg="Never saw '%s' message " % self.processing_message + str(processor.node.account))


        # test done close all down
        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    def verify_processing(self, processor, pattern):
        self.logger.info("Verifying %s processing pattern in STDOUT_FILE" % pattern)
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            monitor.wait_until(pattern,
                               timeout_sec=60,
                               err_msg="Never saw processing of %s " % pattern + str(processor.node.account))

    def all_source_subtopology_tasks(self, processor):
        retries = 0
        while retries < 5:
            found_tasks = processor.node.account.ssh_capture("sed -n 's/.*\(TASK-ASSIGNMENTS:\[^\n\]*\)\1/p' %s" % processor.STDOUT_FILE, allow_fail=True)
            self.logger.info("Returned %s from assigned task check" % found)
            if len(found_tasks) > 0:
                return True
            retries += 1
            time.sleep(1)

        return False

    def set_props(self, processor, upgrade_from=None):
        processor.SOURCE_TOPIC = self.source_topic
        processor.SINK_TOPIC = self.sink_topic
        processor.THREAD_DELIMITER = self.thread_delimiter
        processor.TASK_DELIMITER = self.task_delimiter
        processor.REPORT_INTERVAL = self.report_interval
        processor.UPGRADE_FROM = upgrade_from
