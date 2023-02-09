package kafka.server

import org.apache.kafka.common.header.Header

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
// TODO: Rewrite in Java and move it to a public package
abstract class ProduceRequestInterceptor {
  // Custom function for mutating the original message. If the method returns a ProduceRequestInterceptorSkipRecordException,
  // the record will be removed from the batch and won't be persisted in the target log. All other exceptions are
  // considered "fatal" and will result in a request error
  def processRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, headers: Array[Header]): ProduceRequestInterceptorResult

  // Method that gets called during the interceptor's initialization to configure itself
  def configure(): Unit
}

case class ProduceRequestInterceptorResult(key: Array[Byte], value: Array[Byte])

case class ProduceRequestInterceptorSkipRecordException(msg: String) extends Exception(msg)
