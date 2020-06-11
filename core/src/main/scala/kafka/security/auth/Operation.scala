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
import org.apache.kafka.common.acl.AclOperation

/**
 * Different operations a client may perform on kafka resources.
 */
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
sealed trait Operation extends BaseEnum {
  def toJava : AclOperation
}

@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Read extends Operation {
  val name = "Read"
  val toJava = AclOperation.READ
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Write extends Operation {
  val name = "Write"
  val toJava = AclOperation.WRITE
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Create extends Operation {
  val name = "Create"
  val toJava = AclOperation.CREATE
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Delete extends Operation {
  val name = "Delete"
  val toJava = AclOperation.DELETE
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Alter extends Operation {
  val name = "Alter"
  val toJava = AclOperation.ALTER
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object Describe extends Operation {
  val name = "Describe"
  val toJava = AclOperation.DESCRIBE
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object ClusterAction extends Operation {
  val name = "ClusterAction"
  val toJava = AclOperation.CLUSTER_ACTION
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object DescribeConfigs extends Operation {
  val name = "DescribeConfigs"
  val toJava = AclOperation.DESCRIBE_CONFIGS
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object AlterConfigs extends Operation {
  val name = "AlterConfigs"
  val toJava = AclOperation.ALTER_CONFIGS
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object IdempotentWrite extends Operation {
  val name = "IdempotentWrite"
  val toJava = AclOperation.IDEMPOTENT_WRITE
}
@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
case object All extends Operation {
  val name = "All"
  val toJava = AclOperation.ALL
}

@deprecated("Use org.apache.kafka.common.acl.AclOperation", "Since 2.5")
object Operation {

  def fromString(operation: String): Operation = {
    val op = values.find(op => op.name.equalsIgnoreCase(operation))
    op.getOrElse(throw new KafkaException(operation + " not a valid operation name. The valid names are " + values.mkString(",")))
  }

  def fromJava(operation: AclOperation): Operation = {
    operation match {
      case AclOperation.READ => Read
      case AclOperation.WRITE => Write
      case AclOperation.CREATE => Create
      case AclOperation.DELETE => Delete
      case AclOperation.ALTER => Alter
      case AclOperation.DESCRIBE => Describe
      case AclOperation.CLUSTER_ACTION => ClusterAction
      case AclOperation.ALTER_CONFIGS => AlterConfigs
      case AclOperation.DESCRIBE_CONFIGS => DescribeConfigs
      case AclOperation.IDEMPOTENT_WRITE => IdempotentWrite
      case AclOperation.ALL => All
      case _ => throw new KafkaException(operation + " is not a convertible operation name. The valid names are " + values.mkString(","))
    }
  }

  def values: Seq[Operation] = List(Read, Write, Create, Delete, Alter, Describe, ClusterAction, AlterConfigs,
     DescribeConfigs, IdempotentWrite, All)
}
