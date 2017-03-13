/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.io.IOException
import java.net.ServerSocket
import java.util.Properties

import kafka.utils.TestUtils

/**
 * DO NOT USE THESE UTILITIES UNLESS YOU ABSOLUTELY MUST
 *
 * These are utilities for selecting fixed (preselected), ephemeral ports to use with tests. This is not a reliable way
 * of testing on most machines because you can easily run into port conflicts. If you're using this class, you're almost
 * certainly doing something wrong unless you can prove that your test **cannot function** properly without it.
 */
object FixedPortTestUtils {
  def choosePorts(count: Int): Seq[Int] = {
    try {
      val sockets = (0 until count).map(_ => new ServerSocket(0))
      val ports = sockets.map(_.getLocalPort())
      sockets.foreach(_.close())
      ports
    } catch {
      case e: IOException => throw new RuntimeException(e)
    }
  }

  def createBrokerConfigs(numConfigs: Int,
    zkConnect: String,
    enableControlledShutdown: Boolean = true,
    enableDeleteTopic: Boolean = false): Seq[Properties] = {
    val ports = FixedPortTestUtils.choosePorts(numConfigs)
    (0 until numConfigs).map { node =>
      TestUtils.createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, ports(node))
    }
  }

}
