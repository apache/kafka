package kafka

import kafka.tools.StorageTool
import kafka.utils.Logging

object KafkaNativeWrapper extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {

    }
    val operation = args.head
    val arguments = args.tail
    operation match {
      case "storage-tool" => StorageTool.process(arguments)
      case "kafka" => Kafka.process(arguments)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'storage-tool' or 'kafka'.")
    }
  }
}