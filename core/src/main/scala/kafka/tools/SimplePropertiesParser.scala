package kafka.tools

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.control.NonFatal

object SimplePropertiesParser {

  /**
   * Take a simple string of the form key=val,key=val,key=val and parse into a map. Useful for command line
   * arguments.
   * @param freeform String of format "key=val,key=val,key=val"
   * @return
   */
  def propsFromFreeform(freeform: String): java.util.Map[String, String] = {
    val argProps: immutable.Map[String, String] = freeform.split(",")
      .map(_.split("="))
      .map {
        case Array(k, v) => (k, v)
        case _ => exception(freeform)
    }.toMap
    argProps.asJava
  }

  def exception(freeform: String): Nothing = {
    throw new IllegalArgumentException("There was a problem parsing freeform properties string. These should be of the form key=val,key=val... Input was: " + freeform)
  }
}
