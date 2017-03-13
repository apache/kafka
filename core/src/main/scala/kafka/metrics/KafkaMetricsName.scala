/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.metrics

import com.codahale.metrics.MetricRegistry

//MetricName(group, typeName, name, scope, nameBuilder.toString())
class KafkaMetricsName(val group: String, val typeName: String, val name: String, val scope: String = null, val commonName: String = null) {

  override def toString: String = {
    MetricRegistry.name(name, group, typeName, scope, commonName)
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: KafkaMetricsName =>
      that.group.equals(group) &&
        that.name.equals(name) &&
        that.typeName.equals(typeName) &&
        that.scope.equals(typeName)
    case _ => false
  }
}

object KafkaMetricsName {
  def fromString(str: String): KafkaMetricsName = {
    val components = str.split(",")
    val args: Map[String, String] = components.map(v => {
      val keyValueStr = v.split("=")
      keyValueStr.head -> keyValueStr.last
    }).toMap
    new KafkaMetricsName(args.get("group").orNull, args.get("type").orNull, args.get("name").orNull, args.get("scope").orNull)
  }
}
