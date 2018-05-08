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

import org.apache.kafka.common.record.RecordFormat

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

  private val versionNameMap = Map(
    "0.8.0" -> KAFKA_0_8_0,
    "0.8.1" -> KAFKA_0_8_1,
    "0.8.2" -> KAFKA_0_8_2,
    "0.9.0" -> KAFKA_0_9_0,
    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    "0.10.0-IV0" -> KAFKA_0_10_0_IV0,
    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    "0.10.0-IV1" -> KAFKA_0_10_0_IV1,
    "0.10.0" -> KAFKA_0_10_0_IV1,

    // introduced for JoinGroup protocol change in KIP-62
    "0.10.1-IV0" -> KAFKA_0_10_1_IV0,
    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    "0.10.1-IV1" -> KAFKA_0_10_1_IV1,
    // introduced ListOffsetRequest v1 in KIP-79
    "0.10.1-IV2" -> KAFKA_0_10_1_IV2,
    "0.10.1" -> KAFKA_0_10_1_IV2,
    // introduced UpdateMetadataRequest v3 in KIP-103
    "0.10.2-IV0" -> KAFKA_0_10_2_IV0,
    "0.10.2" -> KAFKA_0_10_2_IV0,
    // KIP-98 (idempotent and transactional producer support)
    "0.11.0-IV0" -> KAFKA_0_11_0_IV0,
    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    "0.11.0-IV1" -> KAFKA_0_11_0_IV1,
    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    "0.11.0-IV2" -> KAFKA_0_11_0_IV2,
    "0.11.0" -> KAFKA_0_11_0_IV2,
    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    "1.0-IV0" -> KAFKA_1_0_IV0,
    "1.0" -> KAFKA_1_0_IV0,
    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    "1.1-IV0" -> KAFKA_1_1_IV0,
    "1.1" -> KAFKA_1_1_IV0,
    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279
    "2.0-IV0" -> KAFKA_2_0_IV0,
    "2.0" -> KAFKA_2_0_IV0
  )

  private val versionPattern = "\\.".r

  def apply(version: String): ApiVersion = {
    val versionsSeq = versionPattern.split(version)
    val numSegments = if (version.startsWith("0.")) 3 else 2
    val key = versionsSeq.take(numSegments).mkString(".")
    versionNameMap.getOrElse(key, throw new IllegalArgumentException(s"Version `$version` is not a valid version"))
  }

  def latestVersion = versionNameMap.values.max

  def allVersions: Set[ApiVersion] = {
    versionNameMap.values.toSet
  }

  def minVersionForMessageFormat(messageFormatVersion: RecordFormat): String = {
    messageFormatVersion match {
      case RecordFormat.V0 => "0.8.0"
      case RecordFormat.V1 => "0.10.0"
      case RecordFormat.V2 => "0.11.0"
      case _ => throw new IllegalArgumentException(s"Invalid message format version $messageFormatVersion")
    }
  }
}

sealed trait ApiVersion extends Ordered[ApiVersion] {
  val version: String
  val messageFormatVersion: RecordFormat
  val id: Int

  override def compare(that: ApiVersion): Int =
    ApiVersion.orderingByVersion.compare(this, that)

  override def toString: String = version
}

// Keep the IDs in order of versions
case object KAFKA_0_8_0 extends ApiVersion {
  val version: String = "0.8.0.X"
  val messageFormatVersion = RecordFormat.V0
  val id: Int = 0
}

case object KAFKA_0_8_1 extends ApiVersion {
  val version: String = "0.8.1.X"
  val messageFormatVersion = RecordFormat.V0
  val id: Int = 1
}

case object KAFKA_0_8_2 extends ApiVersion {
  val version: String = "0.8.2.X"
  val messageFormatVersion = RecordFormat.V0
  val id: Int = 2
}

case object KAFKA_0_9_0 extends ApiVersion {
  val version: String = "0.9.0.X"
  val messageFormatVersion = RecordFormat.V0
  val id: Int = 3
}

case object KAFKA_0_10_0_IV0 extends ApiVersion {
  val version: String = "0.10.0-IV0"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 4
}

case object KAFKA_0_10_0_IV1 extends ApiVersion {
  val version: String = "0.10.0-IV1"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 5
}

case object KAFKA_0_10_1_IV0 extends ApiVersion {
  val version: String = "0.10.1-IV0"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 6
}

case object KAFKA_0_10_1_IV1 extends ApiVersion {
  val version: String = "0.10.1-IV1"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 7
}

case object KAFKA_0_10_1_IV2 extends ApiVersion {
  val version: String = "0.10.1-IV2"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 8
}

case object KAFKA_0_10_2_IV0 extends ApiVersion {
  val version: String = "0.10.2-IV0"
  val messageFormatVersion = RecordFormat.V1
  val id: Int = 9
}

case object KAFKA_0_11_0_IV0 extends ApiVersion {
  val version: String = "0.11.0-IV0"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 10
}

case object KAFKA_0_11_0_IV1 extends ApiVersion {
  val version: String = "0.11.0-IV1"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 11
}

case object KAFKA_0_11_0_IV2 extends ApiVersion {
  val version: String = "0.11.0-IV2"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 12
}

case object KAFKA_1_0_IV0 extends ApiVersion {
  val version: String = "1.0-IV0"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 13
}

case object KAFKA_1_1_IV0 extends ApiVersion {
  val version: String = "1.1-IV0"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 14
}

case object KAFKA_2_0_IV0 extends ApiVersion {
  val version: String = "2.0-IV0"
  val messageFormatVersion = RecordFormat.V2
  val id: Int = 15
}
