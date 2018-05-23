package kafka.zk

import org.junit.{Assert, Test}

class ZkDataTest {

  @Test
  def testConsumerOffsetPath(): Unit = {
    def getConsumersOffsetsZkPath(consumerGroup: String, topic: String, partition: Int): String = {
      s"/consumers/$consumerGroup/offsets/$topic/$partition"
    }

    val consumerGroup = "test-group"
    val topic = "test-topic"
    val partition = 2

    val expectedConsumerGroupOffsetsPath = getConsumersOffsetsZkPath(consumerGroup, topic, partition)
    val actualConsumerGroupOffsetsPath = ConsumerOffset.path(consumerGroup, topic, partition)

    Assert.assertEquals(expectedConsumerGroupOffsetsPath, actualConsumerGroupOffsetsPath)
  }
}