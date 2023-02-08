package kafka.server

/**
 * ProduceRequestInterceptors can be defined to perform custom, light-weight processing on every record received by a
 * broker.
 *
 * Broker-side interceptors should be used with caution:
 *  1. Processing messages that were sent in compressed format by the producer will need to be decompressed and then
 *    re-compressed to perform broker-side processing
 *  2. Performing unduly long or complex computations can negatively impact overall cluster health and performance
 *
 * Potential use cases:
 *   - Schema validation
 *   - Privacy enforcement
 *   - Decoupling server-side and client-side serialization
 */
abstract class ProduceRequestInterceptor {
  // Custom function for mutating the original message key. If the method returns a ProduceRequestInterceptorException,
  // the record will be removed from the batch and won't be persisted in the target log. All other exceptions are
  // considered "fatal". If a fatal exception is encountered, the broker will use the original batch of messages, and
  // ignore the interceptor.
  def processKey(key: Array[Byte]): Array[Byte]
  // Custom function for mutating the original message value. If the method returns a ProduceRequestInterceptorException,
  // the record will be removed from the batch and won't be persisted in the target log. All other exceptions are
  // considered "fatal". If a fatal exception is encountered, the broker will use the original batch of messages, and
  // ignore the interceptor.
  def processValue(value: Array[Byte]): Array[Byte]
  // Method that gets called during the interceptor's initialization to configure itself
  def configure(): Unit
}

class ProduceRequestInterceptorException(msg: String) extends Exception(msg)

class NoOpProduceRequestInterceptor extends ProduceRequestInterceptor {
  override def processKey(key: Array[Byte]): Array[Byte] = {
    if (key == null) null
    else key
  }

  override def processValue(value: Array[Byte]): Array[Byte] = {
    if (value == null) null
    else value
  }

  override def configure(): Unit = ()
}
