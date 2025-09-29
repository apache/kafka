/*
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
package kafka.server.metadata

import kafka.server.ClientQuotaManager
import org.apache.kafka.image.ClientQuotaDelta
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable


class ClientQuotaMetadataManagerTest {

  @Test
  def testHandleIpQuota(): Unit = {
    val manager = new ClientQuotaMetadataManager(null, null)
    assertThrows(classOf[IllegalArgumentException], () => manager.handleIpQuota(IpEntity("a"), new ClientQuotaDelta(null)))
    assertThrows(classOf[IllegalStateException], () => manager.handleIpQuota(UserEntity("a"), new ClientQuotaDelta(null)))
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(DefaultIpEntity, new ClientQuotaDelta(null)) } }
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(IpEntity("192.168.1.1"), new ClientQuotaDelta(null)) } }
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(IpEntity("2001:db8::1"), new ClientQuotaDelta(null)) } }
  }

  @Test
  def testTransferToClientQuotaEntity(): Unit = {
    
    assertThrows(classOf[IllegalStateException],() => ClientQuotaMetadataManager.transferToClientQuotaEntity(IpEntity("a")))
    assertThrows(classOf[IllegalStateException],() => ClientQuotaMetadataManager.transferToClientQuotaEntity(DefaultIpEntity))
    assertEquals(
      (Some(ClientQuotaManager.UserEntity("user")), None),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(UserEntity("user"))
    )
    assertEquals(
      (Some(ClientQuotaManager.DefaultUserEntity), None),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(DefaultUserEntity)
    )
    assertEquals(
      (None, Some(ClientQuotaManager.ClientIdEntity("client"))),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(ClientIdEntity("client"))
    )
    assertEquals(
      (None, Some(ClientQuotaManager.DefaultClientIdEntity)),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(DefaultClientIdEntity)
    )
    assertEquals(
      (Some(ClientQuotaManager.UserEntity("user")), Some(ClientQuotaManager.ClientIdEntity("client"))),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(ExplicitUserExplicitClientIdEntity("user", "client"))
    )
    assertEquals(
      (Some(ClientQuotaManager.UserEntity("user")), Some(ClientQuotaManager.DefaultClientIdEntity)),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(ExplicitUserDefaultClientIdEntity("user"))
    )
    assertEquals(
      (Some(ClientQuotaManager.DefaultUserEntity), Some(ClientQuotaManager.ClientIdEntity("client"))),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(DefaultUserExplicitClientIdEntity("client"))
    )
    assertEquals(
      (Some(ClientQuotaManager.DefaultUserEntity), Some(ClientQuotaManager.DefaultClientIdEntity)),
      ClientQuotaMetadataManager.transferToClientQuotaEntity(DefaultUserDefaultClientIdEntity)
    )
  }
}