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
package kafka.admin

import kafka.admin.TopicCommand.PartitionDescription
import org.apache.kafka.clients.admin.PartitionReassignment
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.junit.Assert._
import org.junit.Test
import scala.jdk.CollectionConverters._

class TopicCommandTest {
  @Test
  def testIsNotUnderReplicatedWhenAdding(): Unit = {
    val replicaIds = List(1, 2)
    val replicas = replicaIds.map { id =>
      new Node(id, "localhost", 9090 + id)
    }

    val partitionDescription = PartitionDescription(
      "test-topic",
      new TopicPartitionInfo(
        0,
        new Node(1, "localhost", 9091),
        replicas.asJava,
        List(new Node(1, "localhost", 9091)).asJava
      ),
      None,
      false,
      Some(
        new PartitionReassignment(
          replicaIds.map(id => id: java.lang.Integer).asJava,
          List(2: java.lang.Integer).asJava,
          List.empty.asJava
        )
      )
    )

    assertFalse(partitionDescription.isUnderReplicated)
  }
}
