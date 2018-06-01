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

sealed trait ResourceNameType extends BaseEnum {
  def toJava: JResourceNameType
}

case object Literal extends ResourceNameType {
  val name = "Literal"
  val toJava = JResourceNameType.LITERAL
}

case object WildcardSuffixed extends ResourceNameType {
  val name = "WildcardSuffixed"
  val toJava = JResourceNameType.WILDCARD_SUFFIXED
}

object ResourceNameType {

  def fromString(resourceNameType: String): ResourceNameType = {
    val rType = values.find(rType => rType.name.equalsIgnoreCase(resourceNameType))
    rType.getOrElse(throw new KafkaException(resourceNameType + " not a valid resourceNameType name. The valid names are " + values.mkString(",")))
  }

  def values: Seq[ResourceNameType] = List(Literal, WildcardSuffixed)

  def fromJava(operation: JResourceNameType): ResourceNameType = fromString(operation.toString.replaceAll("_", ""))
}
