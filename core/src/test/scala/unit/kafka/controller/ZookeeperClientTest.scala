package kafka.controller

import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import kafka.zk.ZooKeeperTestHarness
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.Test

class ZookeeperClientTest extends ZooKeeperTestHarness {
  private val mockPath = "/foo"

  @Test(expected = classOf[UnknownHostException])
  def testUnresolvableConnectString(): Unit = {
    new ZookeeperClient("-1", -1, -1, null, null)
  }

  @Test(expected = classOf[ZookeeperClientTimeoutException])
  def testConnectionTimeout(): Unit = {
    zookeeper.shutdown()
    new ZookeeperClient(zkConnect, zkSessionTimeout, connectionTimeoutMs = 100, null, null)
  }

  @Test
  def testConnection(): Unit = {
    new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
  }

  @Test
  def testDeleteNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val deleteResponse = zookeeperClient.handle(DeleteRequest(mockPath, -1, null)).asInstanceOf[DeleteResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(deleteResponse.rc))
  }

  @Test
  def testDeleteExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val deleteResponse = zookeeperClient.handle(DeleteRequest(mockPath, -1, null)).asInstanceOf[DeleteResponse]
    assertEquals("Response code for delete should be OK", Code.OK, Code.get(deleteResponse.rc))
  }

  @Test
  def testExistsNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val existsResponse = zookeeperClient.handle(ExistsRequest(mockPath, null)).asInstanceOf[ExistsResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(existsResponse.rc))
  }

  @Test
  def testExistsExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val existsResponse = zookeeperClient.handle(ExistsRequest(mockPath, null)).asInstanceOf[ExistsResponse]
    assertEquals("Response code for exists should be OK", Code.OK, Code.get(existsResponse.rc))
  }

  @Test
  def testGetDataNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val getDataResponse = zookeeperClient.handle(GetDataRequest(mockPath, null)).asInstanceOf[GetDataResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(getDataResponse.rc))
  }

  @Test
  def testGetDataExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val data = bytes
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val getDataResponse = zookeeperClient.handle(GetDataRequest(mockPath, null)).asInstanceOf[GetDataResponse]
    assertEquals("Response code for getData should be OK", Code.OK, Code.get(getDataResponse.rc))
    assertArrayEquals("Data for getData should match created znode data", data, getDataResponse.data)
  }

  @Test
  def testSetDataNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val setDataResponse = zookeeperClient.handle(SetDataRequest(mockPath, Array.empty[Byte], -1, null)).asInstanceOf[SetDataResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(setDataResponse.rc))
  }

  @Test
  def testSetDataExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val data = bytes
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val setDataResponse = zookeeperClient.handle(SetDataRequest(mockPath, data, -1, null)).asInstanceOf[SetDataResponse]
    assertEquals("Response code for setData should be OK", Code.OK, Code.get(setDataResponse.rc))
    val getDataResponse = zookeeperClient.handle(GetDataRequest(mockPath, null)).asInstanceOf[GetDataResponse]
    assertEquals("Response code for getData should be OK", Code.OK, Code.get(getDataResponse.rc))
    assertArrayEquals("Data for getData should match setData's data", data, getDataResponse.data)
  }

  @Test
  def testGetACLNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val getACLResponse = zookeeperClient.handle(GetACLRequest(mockPath, null)).asInstanceOf[GetACLResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(getACLResponse.rc))
  }

  @Test
  def testGetACLExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val getACLResponse = zookeeperClient.handle(GetACLRequest(mockPath, null)).asInstanceOf[GetACLResponse]
    assertEquals("Response code for getACL should be OK", Code.OK, Code.get(getACLResponse.rc))
    assertEquals("ACL should be " + ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, getACLResponse.acl)
  }

  @Test
  def testSetACLNonExistentZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val setACLResponse = zookeeperClient.handle(SetACLRequest(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, -1, null)).asInstanceOf[SetACLResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(setACLResponse.rc))
  }

  @Test
  def testGetChildrenNonExistentZNode(): Unit = {
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val getChildrenResponse = zookeeperClient.handle(GetChildrenRequest(mockPath, null)).asInstanceOf[GetChildrenResponse]
    assertEquals("Response code should be NONODE", Code.NONODE, Code.get(getChildrenResponse.rc))
  }

  @Test
  def testGetChildrenExistingZNode(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val getChildrenResponse = zookeeperClient.handle(GetChildrenRequest(mockPath, null)).asInstanceOf[GetChildrenResponse]
    assertEquals("Response code for getChildren should be OK", Code.OK, Code.get(getChildrenResponse.rc))
    assertEquals("getChildren should return no children", Seq.empty[String], getChildrenResponse.children)
  }

  @Test
  def testGetChildrenExistingZNodeWithChildren(): Unit = {
    import scala.collection.JavaConverters._
    val child1 = "child1"
    val child2 = "child2"
    val child1Path = mockPath + "/" + child1
    val child2Path = mockPath + "/" + child2
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val createResponseChild1 = zookeeperClient.handle(CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create child1 should be OK", Code.OK, Code.get(createResponseChild1.rc))
    val createResponseChild2 = zookeeperClient.handle(CreateRequest(child2Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create child2 should be OK", Code.OK, Code.get(createResponseChild2.rc))

    val getChildrenResponse = zookeeperClient.handle(GetChildrenRequest(mockPath, null)).asInstanceOf[GetChildrenResponse]
    assertEquals("Response code for getChildren should be OK", Code.OK, Code.get(getChildrenResponse.rc))
    assertEquals("getChildren should return two children", Seq(child1, child2), getChildrenResponse.children.sorted)
  }

  @Test
  def testPipelinedGetData(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createRequests = (1 to 3).map(x => CreateRequest("/" + x, (x * 2).toString.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    val createResponses = createRequests.map(zookeeperClient.handle)
    createResponses.foreach(createResponse => assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc)))
    val getDataRequests = (1 to 3).map(x => GetDataRequest("/" + x, null))
    val getDataResponses = zookeeperClient.handle(getDataRequests)
    getDataResponses.foreach(getDataResponse => assertEquals("Response code for getData should be OK", Code.OK, Code.get(getDataResponse.rc)))
    getDataResponses.zipWithIndex.foreach { case (getDataResponse, i) =>
      assertEquals("Response code for getData should be OK", Code.OK, Code.get(getDataResponse.rc))
      assertEquals("Data for getData should match", ((i + 1) * 2), Integer.valueOf(new String(getDataResponse.asInstanceOf[GetDataResponse].data)))
    }
  }

  @Test
  def testMixedPipeline(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    val getDataRequest = GetDataRequest(mockPath, null)
    val setDataRequest = SetDataRequest("/nonexistent", Array.empty[Byte], -1, null)
    val responses = zookeeperClient.handle(Seq(getDataRequest, setDataRequest))
    assertEquals("Response code for getData should be OK", Code.OK, Code.get(responses.head.rc))
    assertArrayEquals("Data for getData should be empty", Array.empty[Byte], responses.head.asInstanceOf[GetDataResponse].data)
    assertEquals("Response code for setData should be NONODE", Code.NONODE, Code.get(responses.last.rc))
  }

  @Test
  def testZNodeChangeHandlerForDeletion(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val dataChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation = {}
      override def handleDeletion = {
        dataChangeHandlerCountDownLatch.countDown()
      }
      override def handleDataChange = {}
      override val path: String = mockPath
    }

    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    zookeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val deleteResponse = zookeeperClient.handle(DeleteRequest(mockPath, -1, null)).asInstanceOf[DeleteResponse]
    assertEquals("Response code for delete should be OK", Code.OK, Code.get(deleteResponse.rc))
    assertTrue("Failed to receive delete notification", dataChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChangeHandlerForDataChange(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation = {}
      override def handleDeletion = {}
      override def handleDataChange = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    zookeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val setDataResponse = zookeeperClient.handle(SetDataRequest(mockPath, Array.empty[Byte], -1, null)).asInstanceOf[SetDataResponse]
    assertEquals("Response code for setData should be OK", Code.OK, Code.get(setDataResponse.rc))
    assertTrue("Failed to receive data change notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChildChangeHandlerForChildChange(): Unit = {
    import scala.collection.JavaConverters._
    val zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null, null)
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zookeeperClient.handle(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create should be OK", Code.OK, Code.get(createResponse.rc))
    zookeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val createResponseChild1 = zookeeperClient.handle(CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT, null))
    assertEquals("Response code for create child1 should be OK", Code.OK, Code.get(createResponseChild1.rc))
    assertTrue("Failed to receive child change notification", zNodeChildChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  private def bytes = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
}
