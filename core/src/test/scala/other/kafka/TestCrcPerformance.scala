package kafka.log

import java.util.Random
import kafka.message._
import kafka.utils.{TestUtils, Utils}

object TestCrcPerformance {

  def main(args: Array[String]): Unit = {
    if(args.length < 2)
      Utils.croak("USAGE: java " + getClass().getName() + " num_messages message_size")
    val numMessages = args(0).toInt
    val messageSize = args(1).toInt
    //val numMessages = 100000000
    //val messageSize = 32

    val dir = TestUtils.tempDir()
    val content = new Array[Byte](messageSize)
    new Random(1).nextBytes(content)

    // create message test
    val start = System.nanoTime
    for(i <- 0 until numMessages) {
      new Message(content)
    }
    val ellapsed = System.nanoTime - start
    println("%d messages created in %.2f seconds + (%.2f ns per message).".format(numMessages, ellapsed/(1000.0*1000.0*1000.0),
      ellapsed / numMessages.toDouble))

  }
}
