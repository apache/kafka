package kafka.zookeeper

case class ZooKeeperClientException(message: String) extends RuntimeException(message)