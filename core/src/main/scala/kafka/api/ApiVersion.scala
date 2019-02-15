/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.api

import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.record.RecordVersion

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 *
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another, if it has a higher ID (to avoid depending on lexicographic order)
 *
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 0.10.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 0.10.0, we will add a config value "0.10.0-IV0" and a
 * corresponding case object KAFKA_0_10_0-IV0. We will also add a config value "0.10.0" that will be mapped to the
 * latest internal version object, which is KAFKA_0_10_0-IV0. When we change the protocol a second time while developing
 * 0.10.0, we will add a new config value "0.10.0-IV1" and a corresponding case object KAFKA_0_10_0-IV1. We will change
 * the config value "0.10.0" to map to the latest internal version object KAFKA_0_10_0-IV1. The config value of
 * "0.10.0-IV0" is still mapped to KAFKA_0_10_0-IV0. This way, if people are deploying from trunk, they can use
 * "0.10.0-IV0" and "0.10.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "0.10.0" when upgrading to the 0.10.0 release.
 */
object ApiVersion {
  // This implicit is necessary due to: https://issues.scala-lang.org/browse/SI-8541
  implicit def orderingByVersion[A <: ApiVersion]: Ordering[A] = Ordering.by(_.id)

  val allVersions: Seq[ApiVersion] = Seq(
    KAFKA_0_8_0,
    KAFKA_0_8_1,
    KAFKA_0_8_2,
    KAFKA_0_9_0,
    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    KAFKA_0_10_0_IV0,
    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    KAFKA_0_10_0_IV1,
    // introduced for JoinGroup protocol change in KIP-62
    KAFKA_0_10_1_IV0,
    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    KAFKA_0_10_1_IV1,
    // introduced ListOffsetRequest v1 in KIP-79
    KAFKA_0_10_1_IV2,
    // introduced UpdateMetadataRequest v3 in KIP-103
    KAFKA_0_10_2_IV0,
    // KIP-98 (idempotent and transactional producer support)
    KAFKA_0_11_0_IV0,
    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    KAFKA_0_11_0_IV1,
    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    KAFKA_0_11_0_IV2,
    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    KAFKA_1_0_IV0,
    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    KAFKA_1_1_IV0,
    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader and follower after fast leader fail over)
    KAFKA_2_0_IV0,
    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    KAFKA_2_0_IV1,
    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    KAFKA_2_1_IV0,
    // New Fetch, OffsetsForLeaderEpoch, and ListOffsets schemas (KIP-320)
    KAFKA_2_1_IV1,
    // Support ZStandard Compression Codec (KIP-110)
    KAFKA_2_1_IV2,
    // Introduced broker generation (KIP-380), and
    // LeaderAdnIsrRequest V2, UpdateMetadataRequest V5, StopReplicaRequest V1
    KAFKA_2_2_IV0,
    // New error code for ListOffsets when a new leader is lagging behind former HW (KIP-207)
    KAFKA_2_2_IV1
  )

  // Map keys are the union of the short and full versions
  private val versionMap: Map[String, ApiVersion] =
    allVersions.map(v => v.version -> v).toMap ++ allVersions.groupBy(_.shortVersion).map { case (k, v) => k -> v.last }

  /**
   * Return an `ApiVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
   * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `ApiVersion`.
   */
  def apply(versionString: String): ApiVersion = {
    val versionSegments = versionString.split('.').toSeq
    val numSegments = if (versionString.startsWith("0.")) 3 else 2
    val key = versionSegments.take(numSegments).mkString(".")
    versionMap.getOrElse(key, throw new IllegalArgumentException(s"Version `$versionString` is not a valid version"))
  }

  def latestVersion: ApiVersion = allVersions.last

  /**
   * Return the minimum `ApiVersion` that supports `RecordVersion`.
   */
  def minSupportedFor(recordVersion: RecordVersion): ApiVersion = {
    recordVersion match {
      case RecordVersion.V0 => KAFKA_0_8_0
      case RecordVersion.V1 => KAFKA_0_10_0_IV0
      case RecordVersion.V2 => KAFKA_0_11_0_IV0
      case _ => throw new IllegalArgumentException(s"Invalid message format version $recordVersion")
    }
  }
}

sealed trait ApiVersion extends Ordered[ApiVersion] {
  def version: String
  def shortVersion: String
  def recordVersion: RecordVersion
  def id: Int

  override def compare(that: ApiVersion): Int =
    ApiVersion.orderingByVersion.compare(this, that)

  override def toString: String = version
}

/**
 * For versions before 0.10.0, `version` and `shortVersion` were the same.
 */
sealed trait LegacyApiVersion extends ApiVersion {
  def version = shortVersion
}

/**
 * From 0.10.0 onwards, each version has a sub-version. For example, IV0 is the sub-version of 0.10.0-IV0.
 */
sealed trait DefaultApiVersion extends ApiVersion {
  lazy val version = shortVersion + "-" + subVersion
  protected def subVersion: String
}

// Keep the IDs in order of versions
case object KAFKA_0_8_0 extends LegacyApiVersion {
  val shortVersion = "0.8.0"
  val recordVersion = RecordVersion.V0
  val id: Int = 0
}

case object KAFKA_0_8_1 extends LegacyApiVersion {
  val shortVersion = "0.8.1"
  val recordVersion = RecordVersion.V0
  val id: Int = 1
}

case object KAFKA_0_8_2 extends LegacyApiVersion {
  val shortVersion = "0.8.2"
  val recordVersion = RecordVersion.V0
  val id: Int = 2
}

case object KAFKA_0_9_0 extends LegacyApiVersion {
  val shortVersion = "0.9.0"
  val subVersion = ""
  val recordVersion = RecordVersion.V0
  val id: Int = 3
}

case object KAFKA_0_10_0_IV0 extends DefaultApiVersion {
  val shortVersion = "0.10.0"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V1
  val id: Int = 4
}

case object KAFKA_0_10_0_IV1 extends DefaultApiVersion {
  val shortVersion = "0.10.0"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V1
  val id: Int = 5
}

case object KAFKA_0_10_1_IV0 extends DefaultApiVersion {
  val shortVersion = "0.10.1"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V1
  val id: Int = 6
}

case object KAFKA_0_10_1_IV1 extends DefaultApiVersion {
  val shortVersion = "0.10.1"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V1
  val id: Int = 7
}

case object KAFKA_0_10_1_IV2 extends DefaultApiVersion {
  val shortVersion = "0.10.1"
  val subVersion = "IV2"
  val recordVersion = RecordVersion.V1
  val id: Int = 8
}

case object KAFKA_0_10_2_IV0 extends DefaultApiVersion {
  val shortVersion = "0.10.2"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V1
  val id: Int = 9
}

case object KAFKA_0_11_0_IV0 extends DefaultApiVersion {
  val shortVersion = "0.11.0"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 10
}

case object KAFKA_0_11_0_IV1 extends DefaultApiVersion {
  val shortVersion = "0.11.0"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V2
  val id: Int = 11
}

case object KAFKA_0_11_0_IV2 extends DefaultApiVersion {
  val shortVersion = "0.11.0"
  val subVersion = "IV2"
  val recordVersion = RecordVersion.V2
  val id: Int = 12
}

case object KAFKA_1_0_IV0 extends DefaultApiVersion {
  val shortVersion = "1.0"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 13
}

case object KAFKA_1_1_IV0 extends DefaultApiVersion {
  val shortVersion = "1.1"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 14
}

case object KAFKA_2_0_IV0 extends DefaultApiVersion {
  val shortVersion: String = "2.0"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 15
}

case object KAFKA_2_0_IV1 extends DefaultApiVersion {
  val shortVersion: String = "2.0"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V2
  val id: Int = 16
}

case object KAFKA_2_1_IV0 extends DefaultApiVersion {
  val shortVersion: String = "2.1"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 17
}

case object KAFKA_2_1_IV1 extends DefaultApiVersion {
  val shortVersion: String = "2.1"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V2
  val id: Int = 18
}

case object KAFKA_2_1_IV2 extends DefaultApiVersion {
  val shortVersion: String = "2.1"
  val subVersion = "IV2"
  val recordVersion = RecordVersion.V2
  val id: Int = 19
}

case object KAFKA_2_2_IV0 extends DefaultApiVersion {
  val shortVersion: String = "2.2"
  val subVersion = "IV0"
  val recordVersion = RecordVersion.V2
  val id: Int = 20
}

case object KAFKA_2_2_IV1 extends DefaultApiVersion {
  val shortVersion: String = "2.2"
  val subVersion = "IV1"
  val recordVersion = RecordVersion.V2
  val id: Int = 21
}

object ApiVersionValidator extends Validator {

  override def ensureValid(name: String, value: Any): Unit = {
    try {
      ApiVersion(value.toString)
    } catch {
      case e: IllegalArgumentException => throw new ConfigException(name, value.toString, e.getMessage)
    }
  }

  override def toString: String = "[" + ApiVersion.allVersions.map(_.version).distinct.mkString(", ") + "]"
}
