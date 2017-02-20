package unit.kafka.admin

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig

/**
  * Created by jeqo on 20.02.17.
  */
class ResetConsumerGroupOffsetTest extends KafkaServerTestHarness {
  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs(): Seq[KafkaConfig] = ???
}
