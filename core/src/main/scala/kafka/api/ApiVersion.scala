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

import kafka.message.Message

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
    "0.10.0-IV0" -> KAFKA_0_10_0_IV0,
    "0.10.0" -> KAFKA_0_10_0_IV0
  )

  private val versionPattern = "\\.".r

  def apply(version: String): ApiVersion =
    versionNameMap.getOrElse(versionPattern.split(version).slice(0, 3).mkString("."),
      throw new IllegalArgumentException(s"Version `$version` is not a valid version"))

  def latestVersion = versionNameMap.values.max

}

sealed trait ApiVersion extends Ordered[ApiVersion] {
  val version: String
  val messageFormatVersion: Byte
  val id: Int

  override def compare(that: ApiVersion): Int =
    ApiVersion.orderingByVersion.compare(this, that)

  override def toString(): String = version
}

// Keep the IDs in order of versions
case object KAFKA_0_8_0 extends ApiVersion {
  val version: String = "0.8.0.X"
  val messageFormatVersion: Byte = Message.MagicValue_V0
  val id: Int = 0
}

case object KAFKA_0_8_1 extends ApiVersion {
  val version: String = "0.8.1.X"
  val messageFormatVersion: Byte = Message.MagicValue_V0
  val id: Int = 1
}

case object KAFKA_0_8_2 extends ApiVersion {
  val version: String = "0.8.2.X"
  val messageFormatVersion: Byte = Message.MagicValue_V0
  val id: Int = 2
}

case object KAFKA_0_9_0 extends ApiVersion {
  val version: String = "0.9.0.X"
  val messageFormatVersion: Byte = Message.MagicValue_V0
  val id: Int = 3
}

case object KAFKA_0_10_0_IV0 extends ApiVersion {
  val version: String = "0.10.0-IV0"
  val messageFormatVersion: Byte = Message.MagicValue_V1
  val id: Int = 4
}
