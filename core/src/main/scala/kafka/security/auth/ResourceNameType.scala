/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.security.auth

import kafka.common.{BaseEnum, KafkaException}
import org.apache.kafka.common.resource.{ResourceNameType => JResourceNameType}

sealed trait ResourceNameType extends BaseEnum  with Ordered[ ResourceNameType ] {
  def toJava: JResourceNameType

  override def compare(that: ResourceNameType): Int = this.name compare that.name
}

case object Literal extends ResourceNameType {
  val name = "Literal"
  val toJava = JResourceNameType.LITERAL
}

case object Prefixed extends ResourceNameType {
  val name = "Prefixed"
  val toJava = JResourceNameType.PREFIXED
}

object ResourceNameType {

  def fromString(resourceNameType: String): ResourceNameType = {
    val rType = values.find(rType => rType.name.equalsIgnoreCase(resourceNameType))
    rType.getOrElse(throw new KafkaException(resourceNameType + " not a valid resourceNameType name. The valid names are " + values.mkString(",")))
  }

  def values: Seq[ResourceNameType] = List(Literal, Prefixed)

  def fromJava(nameType: JResourceNameType): ResourceNameType = fromString(nameType.toString)
}
