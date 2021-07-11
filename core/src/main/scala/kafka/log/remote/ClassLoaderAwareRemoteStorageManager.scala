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

import org.apache.kafka.server.log.remote.storage.{LogSegmentData, RemoteLogSegmentMetadata, RemoteStorageManager}

import java.io.InputStream
import java.util

/**
 * A wrapper class of RemoteStorageManager that sets the context class loader when calling RSM methods.
 */
class ClassLoaderAwareRemoteStorageManager(val rsm: RemoteStorageManager,
                                           val rsmClassLoader: ClassLoader) extends RemoteStorageManager {

  def withClassLoader[T](fun: => T): T = {
    val originalClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(rsmClassLoader)
    try {
      fun
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }

  def delegate(): RemoteStorageManager = {
    rsm
  }

  override def close(): Unit = withClassLoader {
    rsm.close()
  }

  override def configure(configs: util.Map[String, _]): Unit = withClassLoader {
    rsm.configure(configs)
  }

  override def copyLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                                  logSegmentData: LogSegmentData): Unit = withClassLoader {
    rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData)
  }

  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                               startPosition: Int): InputStream = withClassLoader {
    rsm.fetchLogSegment(remoteLogSegmentMetadata, startPosition)
  }

  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                               startPosition: Int,
                               endPosition: Int): InputStream = withClassLoader {
    rsm.fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition)
  }

  override def fetchIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                          indexType: RemoteStorageManager.IndexType): InputStream = withClassLoader {
    rsm.fetchIndex(remoteLogSegmentMetadata, indexType)
  }

  override def deleteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = withClassLoader {
    rsm.deleteLogSegmentData(remoteLogSegmentMetadata)
  }
}