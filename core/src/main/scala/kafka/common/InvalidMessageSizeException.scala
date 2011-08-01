package kafka.common

/**
 * Indicates the client has requested a range no longer available on the server
 */
class InvalidMessageSizeException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}

