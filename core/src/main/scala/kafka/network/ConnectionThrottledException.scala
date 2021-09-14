package kafka.network

import org.apache.kafka.common.KafkaException

import java.net.InetAddress

class ConnectionThrottledException(val ip: InetAddress, val startThrottleTimeMs: Long, val throttleTimeMs: Long)
  extends KafkaException(s"$ip throttled for $throttleTimeMs")
