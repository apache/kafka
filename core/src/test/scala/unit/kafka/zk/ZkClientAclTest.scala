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
package kafka.zk

import java.security.MessageDigest
import java.util.Base64

import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.{ACL, Id}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ZkClientAclTest extends ZooKeeperTestHarness {

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
  }

  @Test
  def testChrootExistsAndRootIsLocked(): Unit = {
    // chroot is accessible
    val chroot = "/chroot"
    zkClient.makeSurePersistentPathExists(chroot)
    zkClient.setAcl(chroot, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)

    // root is inaccessible
    val scheme = "digest"
    val id = "test"
    val pwd = "12345"
    val digest = Base64.getEncoder.encode(MessageDigest.getInstance("SHA1").digest(s"$id:$pwd".getBytes()))
    zkClient.currentZooKeeper.addAuthInfo(scheme, digest)
    zkClient.setAcl("/", Seq(new ACL(ZooDefs.Perms.ALL, new Id(scheme, s"$id:$digest"))))

    // this client won't have access to the root, but the chroot already exists
    val chrootClient = KafkaZkClient(zkConnect + chroot, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, createChrootIfNecessary = true)
    chrootClient.close()
  }
}
