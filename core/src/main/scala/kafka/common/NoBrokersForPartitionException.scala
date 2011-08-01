package kafka.common

/**
 * Thrown when a request is made for broker but no brokers with that topic
 * exist.
 */
class NoBrokersForPartitionException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}