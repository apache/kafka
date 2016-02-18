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

package kafka.server

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.integration.KafkaServerTestHarness
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.junit.Before

/**
 * TODO: This is a pseudo-temporary test implementation to test admin requests while we still do not have an AdminClient.
 * Once the AdminClient is added this should be changed to utilize that instead of this custom/duplicated socket code.
 */
class BaseAdminRequestTest extends KafkaServerTestHarness {
    val numBrokers = 3
    def generateConfigs() = {
        val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect)
        props.foreach { p =>
            p.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
            p.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
        }
        props.map(KafkaConfig.fromProps)
    }

    @Before
    override def setUp() {
        super.setUp()
        TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
    }

    def socketServer = {
        servers.head.socketServer
    }

    def broker = {
        val broker = servers.head
        new Broker(broker.config.brokerId, broker.config.hostName, broker.boundPort())
    }

    def connect(s: SocketServer = socketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
        new Socket("localhost", s.boundPort(protocol))
    }

    def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None) {
        val outgoing = new DataOutputStream(socket.getOutputStream)
        id match {
            case Some(id) =>
                outgoing.writeInt(request.length + 2)
                outgoing.writeShort(id)
            case None =>
                outgoing.writeInt(request.length)
        }
        outgoing.write(request)
        outgoing.flush()
    }

    def receiveResponse(socket: Socket): Array[Byte] = {
        val incoming = new DataInputStream(socket.getInputStream)
        val len = incoming.readInt()
        val response = new Array[Byte](len)
            incoming.readFully(response)
        response
    }

    def requestAndReceive(request: Array[Byte], id: Option[Short] = None): Array[Byte] = {
        val plainSocket = connect()
        try {
            sendRequest(plainSocket, request, id)
            receiveResponse(plainSocket)
        } finally {
            plainSocket.close()
        }
    }

    def topicExists(topic: String, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Boolean = {
        val metadata = ClientUtils.fetchTopicMetadata(Set(topic), Seq(broker.getBrokerEndPoint(protocol)),
            "topicExists", 2000, 0).topicsMetadata

        metadata.exists(p => p.topic.equals(topic) && p.errorCode == Errors.NONE.code)
    }
}
