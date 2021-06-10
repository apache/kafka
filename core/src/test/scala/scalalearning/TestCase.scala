package scalalearning

import org.junit.jupiter.api.Test

object TestCase {

  def main(args: Array[String]): Unit = {
    testType()
  }
  /**
   * 简单匹配
   */
  def testSimpleCase(): Unit = {
    val bools = List(true, false, "hello")
    for (bool <- bools) {
      bool match {
        case true => println("true")
        case false => println("false")
        case _ => println("Error input")
      }
    }
  }

  /**
   * 按类型匹配
   */
  def testType(): Unit = {
    val t = List(23, "str",8.5);
    for (temp <- t) {
      temp match {
        case String => println("String")
        case Double => println("Double")
        case Int    => println("Int")
        case _      => println("Nothing")
      }
    }
  }

}
