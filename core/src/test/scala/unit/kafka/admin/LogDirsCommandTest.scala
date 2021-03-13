package unit.kafka.admin

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import kafka.admin.LogDirsCommand
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import scala.collection.Seq

class LogDirsCommandTest extends KafkaServerTestHarness {

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnect)
      .map(KafkaConfig.fromProps)
  }

  @Test
  def checkLogDirsCommandOutput(): Unit = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val printStream = new PrintStream(byteArrayOutputStream, false, StandardCharsets.UTF_8.name())
    //input exist brokerList
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0", "--describe"), printStream)
    val existBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val existBrokersLineIter = existBrokersContent.split("\n").iterator

    assertTrue(existBrokersLineIter.hasNext)
    assertTrue(existBrokersLineIter.next().contains(s"Querying brokers for log directories information"))

    //input nonExist brokerList
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0,1,2", "--describe"), printStream)
    val nonExistBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val nonExistBrokersLineIter = nonExistBrokersContent.split("\n").iterator

    assertTrue(nonExistBrokersLineIter.hasNext)
    assertTrue(nonExistBrokersLineIter.next().contains(s"ERROR: The given node(s) does not exist from broker-list 1,2"))

    //use all brokerList for current cluster
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--describe"), printStream)
    val allBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val allBrokersLineIter = allBrokersContent.split("\n").iterator

    assertTrue(allBrokersLineIter.hasNext)
    assertTrue(allBrokersLineIter.next().contains(s"Querying brokers for log directories information"))
  }
}
