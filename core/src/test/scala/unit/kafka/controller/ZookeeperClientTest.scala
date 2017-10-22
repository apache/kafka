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
package kafka.controller

import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import javax.security.auth.login.Configuration

import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.{After, Test}

class ZookeeperClientTest extends ZooKeeperTestHarness {
  private val mockPath = "/foo"

  @After
  override def tearDown() {
    super.tearDown()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
  }

  @Test(expected = classOf[UnknownHostException])
  def testUnresolvableConnectString(): Unit = {
    new ZookeeperClient("some.invalid.hostname.foo.bar.local", -1, -1, null)
  }

  @Test(expected = classOf[ZookeeperClientTimeoutException])
  def testConnectionTimeout(): Unit = {
    zookeeper.shutdown()
    new ZookeeperClient(zkConnect, zkSessionTimeout, connectionTimeoutMs = 100, null)
  }

  @Test
  def testConnection(): Unit = {
    new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
  }

  @Test
  def testDeleteNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val deleteResponse = zookeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code should be NONODE", Code.NONODE, deleteResponse.resultCode)
  }

  @Test
  def testDeleteExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val deleteResponse = zookeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code for delete should be OK", Code.OK, deleteResponse.resultCode)
  }

  @Test
  def testExistsNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val existsResponse = zookeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, existsResponse.resultCode)
  }

  @Test
  def testExistsExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val existsResponse = zookeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals("Response code for exists should be OK", Code.OK, existsResponse.resultCode)
  }

  @Test
  def testGetDataNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val getDataResponse = zookeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, getDataResponse.resultCode)
  }

  @Test
  def testGetDataExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val data = bytes
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala,
      CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getDataResponse = zookeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
    assertArrayEquals("Data for getData should match created znode data", data, getDataResponse.data)
  }

  @Test
  def testSetDataNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val setDataResponse = zookeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals("Response code should be NONODE", Code.NONODE, setDataResponse.resultCode)
  }

  @Test
  def testSetDataExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val data = bytes
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val setDataResponse = zookeeperClient.handleRequest(SetDataRequest(mockPath, data, -1))
    assertEquals("Response code for setData should be OK", Code.OK, setDataResponse.resultCode)
    val getDataResponse = zookeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
    assertArrayEquals("Data for getData should match setData's data", data, getDataResponse.data)
  }

  @Test
  def testGetAclNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val getAclResponse = zookeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, getAclResponse.resultCode)
  }

  @Test
  def testGetAclExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getAclResponse = zookeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals("Response code for getAcl should be OK", Code.OK, getAclResponse.resultCode)
    assertEquals("ACL should be " + ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, getAclResponse.acl)
  }

  @Test
  def testSetAclNonExistentZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val setAclResponse = zookeeperClient.handleRequest(SetAclRequest(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, -1))
    assertEquals("Response code should be NONODE", Code.NONODE, setAclResponse.resultCode)
  }

  @Test
  def testGetChildrenNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val getChildrenResponse = zookeeperClient.handleRequest(GetChildrenRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, getChildrenResponse.resultCode)
  }

  @Test
  def testGetChildrenExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getChildrenResponse = zookeeperClient.handleRequest(GetChildrenRequest(mockPath))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    assertEquals("getChildren should return no children", Seq.empty[String], getChildrenResponse.children)
  }

  @Test
  def testGetChildrenExistingZNodeWithChildren(): Unit = {
    import scala.collection.JavaConverters._
    val child1 = "child1"
    val child2 = "child2"
    val child1Path = mockPath + "/" + child1
    val child2Path = mockPath + "/" + child2
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val createResponseChild1 = zookeeperClient.handleRequest(CreateRequest(child1Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child1 should be OK", Code.OK, createResponseChild1.resultCode)
    val createResponseChild2 = zookeeperClient.handleRequest(CreateRequest(child2Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child2 should be OK", Code.OK, createResponseChild2.resultCode)

    val getChildrenResponse = zookeeperClient.handleRequest(GetChildrenRequest(mockPath))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    assertEquals("getChildren should return two children", Seq(child1, child2), getChildrenResponse.children.sorted)
  }

  @Test
  def testPipelinedGetData(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createRequests = (1 to 3).map(x => CreateRequest("/" + x, (x * 2).toString.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    val createResponses = createRequests.map(zookeeperClient.handleRequest)
    createResponses.foreach(createResponse => assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode))
    val getDataRequests = (1 to 3).map(x => GetDataRequest("/" + x))
    val getDataResponses = zookeeperClient.handleRequests(getDataRequests)
    getDataResponses.foreach(getDataResponse => assertEquals("Response code for getData should be OK", Code.OK,
      getDataResponse.resultCode))
    getDataResponses.zipWithIndex.foreach { case (getDataResponse, i) =>
      assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
      assertEquals("Data for getData should match", ((i + 1) * 2), Integer.valueOf(new String(getDataResponse.data)))
    }
  }

  @Test
  def testMixedPipeline(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getDataRequest = GetDataRequest(mockPath)
    val setDataRequest = SetDataRequest("/nonexistent", Array.empty[Byte], -1)
    val responses = zookeeperClient.handleRequests(Seq(getDataRequest, setDataRequest))
    assertEquals("Response code for getData should be OK", Code.OK, responses.head.resultCode)
    assertArrayEquals("Data for getData should be empty", Array.empty[Byte], responses.head.asInstanceOf[GetDataResponse].data)
    assertEquals("Response code for setData should be NONODE", Code.NONODE, responses.last.resultCode)
  }

  @Test
  def testZNodeChangeHandlerForCreation(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zookeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zookeeperClient.handleRequests(Seq(existsRequest, createRequest))
    assertEquals("Response code for exists should be NONODE", Code.NONODE, responses.head.resultCode)
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertTrue("Failed to receive create notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChangeHandlerForDeletion(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDeletion(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zookeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zookeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertEquals("Response code for exists should be OK", Code.OK, responses.head.resultCode)
    val deleteResponse = zookeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code for delete should be OK", Code.OK, deleteResponse.resultCode)
    assertTrue("Failed to receive delete notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChangeHandlerForDataChange(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDataChange(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zookeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zookeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertEquals("Response code for exists should be OK", Code.OK, responses.head.resultCode)
    val setDataResponse = zookeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals("Response code for setData should be OK", Code.OK, setDataResponse.resultCode)
    assertTrue("Failed to receive data change notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChildChangeHandlerForChildChange(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange(): Unit = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zookeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    zookeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val getChildrenResponse = zookeeperClient.handleRequest(GetChildrenRequest(mockPath))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    val createResponseChild1 = zookeeperClient.handleRequest(CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child1 should be OK", Code.OK, createResponseChild1.resultCode)
    assertTrue("Failed to receive child change notification", zNodeChildChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testStateChangeHandlerForAuthFailure(): Unit = {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "no-such-file-exists.conf")
    val stateChangeHandlerCountDownLatch = new CountDownLatch(1)
    val stateChangeHandler = new StateChangeHandler {
      override def onAuthFailure(): Unit = {
        stateChangeHandlerCountDownLatch.countDown()
      }
    }
    new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, stateChangeHandler)
    assertTrue("Failed to receive auth failed notification", stateChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  private def bytes = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
}
