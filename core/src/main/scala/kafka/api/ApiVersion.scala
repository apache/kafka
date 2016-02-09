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

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 *
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another, if it has a higher ID (to avoid depending on lexicographic order)
 */
object ApiVersion {
  // This implicit is necessary due to: https://issues.scala-lang.org/browse/SI-8541
  implicit def orderingByVersion[A <: ApiVersion]: Ordering[A] = Ordering.by(_.id)

  private val versionNameMap = Map(
    "0.8.0" -> KAFKA_080,
    "0.8.1" -> KAFKA_081,
    "0.8.2" -> KAFKA_082,
    "0.9.0" -> KAFKA_090,
    "0.9.1" -> KAFKA_091
  )

  def apply(version: String): ApiVersion  = versionNameMap(version.split("\\.").slice(0,3).mkString("."))

  def latestVersion = versionNameMap.values.max
}

sealed trait ApiVersion extends Ordered[ApiVersion] {
  val version: String
  val id: Int

  override def compare(that: ApiVersion): Int = {
    ApiVersion.orderingByVersion.compare(this, that)
  }

  def onOrAfter(that: ApiVersion): Boolean = {
    this.compare(that) >= 0
  }

  override def toString(): String = version
}

// Keep the IDs in order of versions
case object KAFKA_080 extends ApiVersion {
  val version: String = "0.8.0.X"
  val id: Int = 0
}

case object KAFKA_081 extends ApiVersion {
  val version: String = "0.8.1.X"
  val id: Int = 1
}

case object KAFKA_082 extends ApiVersion {
  val version: String = "0.8.2.X"
  val id: Int = 2
}

case object KAFKA_090 extends ApiVersion {
  val version: String = "0.9.0.X"
  val id: Int = 3
}

case object KAFKA_091 extends ApiVersion {
  val version: String = "0.9.1.X"
  val id: Int = 4
}
