/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import org.apache.kafka.server.log.remote.storage.{ClassLoaderAwareRemoteStorageManager, RemoteStorageManager}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.mockito.ArgumentMatchers.any

import java.util.Collections

class ClassLoaderAwareRemoteStorageManagerTest {

  @Test
  def testWithClassLoader(): Unit = {
    val dummyClassLoader = new DummyClassLoader()
    val delegate = mock(classOf[RemoteStorageManager])
    val rsm = new ClassLoaderAwareRemoteStorageManager(delegate, dummyClassLoader)
    when(delegate.configure(any())).thenAnswer(_ =>
      assertEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader))

    assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader)
    rsm.configure(Collections.emptyMap())
    assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader)
  }

  private class DummyClassLoader extends ClassLoader
}
