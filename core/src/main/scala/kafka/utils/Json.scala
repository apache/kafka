package kafka.utils

import kafka.common._
import scala.collection._
import util.parsing.json.JSON

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object Json extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc
  val lock = new Object

  /**
   * Parse a JSON string into an object
   */
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
  
  /**
   * Encode an object into a JSON string. This method accepts any type T where
   *   T => null | Boolean | String | Number | Map[String, T] | Array[T] | Iterable[T]
   * Any other type will result in an exception.
   * 
   * This method does not properly handle non-ascii characters. 
   */
  def encode(obj: Any): String = {
    obj match {
      case null => "null"
      case b: Boolean => b.toString
      case s: String => "\"" + s + "\""
      case n: Number => n.toString
      case m: Map[_, _] => 
        "{" + 
          m.map(elem => 
            elem match {
            case t: Tuple2[_,_] => encode(t._1) + ":" + encode(t._2)
            case _ => throw new IllegalArgumentException("Invalid map element (" + elem + ") in " + obj)
          }).mkString(",") + 
      "}"
      case a: Array[_] => encode(a.toSeq)
      case i: Iterable[_] => "[" + i.map(encode).mkString(",") + "]"
      case other: AnyRef => throw new IllegalArgumentException("Unknown arguement of type " + other.getClass + ": " + other)
    }
  }
}