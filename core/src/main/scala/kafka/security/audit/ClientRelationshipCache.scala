package kafka.security.audit

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}

class ClientRelationshipCache(maxSize: Int) extends RemovalListener[String, util.Set[String]] {

  val cache = Caffeine.newBuilder
    .maximumSize(maxSize)
    .initialCapacity(maxSize / 2)
    .removalListener(this)
    .build[String, util.Set[String]]

  override def onRemoval(key: String, value: util.Set[String], cause: RemovalCause): Unit = {}

  def putRelationship(clientId: String, topic: String): Boolean = {
    val topics: util.Set[String] = cache.get(clientId, _ => ConcurrentHashMap.newKeySet[String])
    topics.add(topic)
  }
}
