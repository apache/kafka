package kafka.tools

import org.junit.Test
import org.junit.Assert.assertEquals

class GetOffsetShellTest {
  @Test
  def testBootstrapServerArgOverridesBrokerListArg(): Unit = {
    val args = Array(
      "--broker-list", "localhost:9091",
      "--bootstrap-server", "localhost:9092",
      "--topic", "top"
    )
    val opts = GetOffsetShell.validateAndParseArgs(args)
    assertEquals("localhost:9092", opts.bootstrapServers)
  }
}
