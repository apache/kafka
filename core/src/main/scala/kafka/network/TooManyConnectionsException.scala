package kafka.network

import org.apache.kafka.common.KafkaException

import java.net.InetAddress

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")
