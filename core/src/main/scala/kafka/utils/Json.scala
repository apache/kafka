package kafka.utils

import kafka.common._
import util.parsing.json.JSON

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object Json extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc
  val lock = new Object

  def parseFull(input: String): Option[Any] = {
    lock synchronized {
      try {
        JSON.parseFull(input)
      } catch {
        case t =>
          throw new KafkaException("Can't parse json string: %s".format(input), t)
      }
    }
  }
}