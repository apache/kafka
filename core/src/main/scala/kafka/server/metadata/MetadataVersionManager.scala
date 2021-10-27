package kafka.server.metadata

import org.apache.kafka.image.FeaturesDelta

import scala.collection.mutable

class MetadataVersionManager {
  @volatile var version: Short = -1

  private val listeners: mutable.Set[MetadataVersionChangeListener] = mutable.HashSet()

  def update(featuresDelta: FeaturesDelta, highestMetadataOffset: Long): Unit = {
    if (featuresDelta.metadataVersionChange().isPresent) {
      val prev = version
      val curr = featuresDelta.metadataVersionChange().get.max
      version = curr
      listeners.foreach { listener =>
        listener.apply(prev, curr)
      }
    }
  }

  def get(): Short = version

  def listen(listener: MetadataVersionChangeListener): Unit = {
    listeners.add(listener)
  }
}

trait MetadataVersionChangeListener {
  def apply(previousVersion: Short, currentVersion: Short): Unit
}