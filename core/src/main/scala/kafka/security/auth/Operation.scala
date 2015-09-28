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
package kafka.security.auth

import kafka.common.{BaseEnum, KafkaException}

/**
 * Different operations a client may perform on kafka resources.
 */

sealed trait Operation extends BaseEnum
case object Read extends Operation { val name = "Read" }
case object Write extends Operation { val name = "Write" }
case object Create extends Operation { val name = "Create" }
case object Delete extends Operation { val name = "Delete" }
case object Alter extends Operation { val name = "Alter" }
case object Describe extends Operation { val name = "Describe" }
case object ClusterAction extends Operation { val name = "ClusterAction" }
case object All extends Operation { val name = "All" }

object Operation {
   def fromString(operation: String): Operation = {
      val op = values.find(op => op.name.equalsIgnoreCase(operation))
      op.getOrElse(throw new KafkaException(operation + " not a valid operation name. The valid names are " + values.mkString(",")))
   }

   def values: Seq[Operation] = List(Read, Write, Create, Delete, Alter, Describe, ClusterAction, All)
}
