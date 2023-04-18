package kafka.migration

import kafka.cluster.Broker
import org.apache.kafka.common.metadata.RegisterBrokerRecord
import org.apache.kafka.image.ClusterImage
import org.apache.kafka.metadata.BrokerRegistration
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class MigrationPropagatorTest {
  def brokerBuilder(brokerId: Int, isZkBroker: Boolean, isFenced: Boolean): BrokerRegistration = {
    BrokerRegistration.fromRecord(
      new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setIsMigratingZkBroker(isZkBroker)
        .setBrokerEpoch(10)
        .setFenced(isFenced)
    )
  }

  def brokersToClusterImage(brokers: Seq[BrokerRegistration]): ClusterImage = {
    val brokerMap = brokers.map(broker => Integer.valueOf(broker.id()) -> broker).toMap.asJava
    new ClusterImage(brokerMap)
  }

  @Test
  def testCalculateBrokerChanges(): Unit = {
    // Start with one fenced, one un-fenced ZK broker
    var broker0 = brokerBuilder(0, true, true)
    var broker1 = brokerBuilder(1, true, false)
    MigrationPropagator.calculateBrokerChanges(ClusterImage.EMPTY, brokersToClusterImage(Seq(broker0, broker1))) match {
      case (addedBrokers, removedBrokers) =>
        assertFalse(addedBrokers.contains(Broker.fromBrokerRegistration(broker0)))
        assertTrue(addedBrokers.contains(Broker.fromBrokerRegistration(broker1)))
        assertTrue(removedBrokers.isEmpty)
    }

    // Un-fence broker 0
    var prevImage = brokersToClusterImage(Seq(broker0, broker1))
    broker0 = brokerBuilder(0, true, false)
    broker1 = brokerBuilder(1, true, false)
    MigrationPropagator.calculateBrokerChanges(prevImage, brokersToClusterImage(Seq(broker0, broker1))) match {
      case (addedBrokers, removedBrokers) =>
        assertTrue(addedBrokers.contains(Broker.fromBrokerRegistration(broker0)))
        assertFalse(addedBrokers.contains(Broker.fromBrokerRegistration(broker1)))
        assertTrue(removedBrokers.isEmpty)
    }

    // Migrate both to KRaft
    prevImage = brokersToClusterImage(Seq(broker0, broker1))
    broker0 = brokerBuilder(0, false, false)
    broker1 = brokerBuilder(1, false, false)
    MigrationPropagator.calculateBrokerChanges(prevImage, brokersToClusterImage(Seq(broker0, broker1))) match {
      case (addedBrokers, removedBrokers) =>
        assertTrue(addedBrokers.isEmpty)
        assertTrue(removedBrokers.contains(Broker.fromBrokerRegistration(broker0)))
        assertTrue(removedBrokers.contains(Broker.fromBrokerRegistration(broker0)))
    }

    // Downgrade one back to ZK
    prevImage = brokersToClusterImage(Seq(broker0, broker1))
    broker0 = brokerBuilder(0, true, false)
    broker1 = brokerBuilder(1, false, false)
    MigrationPropagator.calculateBrokerChanges(prevImage, brokersToClusterImage(Seq(broker0, broker1))) match {
      case (addedBrokers, removedBrokers) =>
        assertTrue(addedBrokers.contains(Broker.fromBrokerRegistration(broker0)))
        assertFalse(addedBrokers.contains(Broker.fromBrokerRegistration(broker1)))
        assertTrue(removedBrokers.isEmpty)
    }
  }
}
