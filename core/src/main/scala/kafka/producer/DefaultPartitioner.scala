/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.producer


import kafka.utils._
import org.apache.kafka.common.utils.Utils

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "It has been replaced by org.apache.kafka.clients.producer.internals.DefaultPartitioner.", "0.10.0.0")
class DefaultPartitioner(props: VerifiableProperties = null) extends Partitioner {
  private val random = new java.util.Random
  
  def partition(key: Any, numPartitions: Int): Int = {
    Utils.abs(key.hashCode) % numPartitions
  }
}
