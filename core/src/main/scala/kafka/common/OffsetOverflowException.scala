package kafka.common

class OffsetOverflowException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
