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

import java.io.InputStream
import java.{lang, util}

import org.apache.kafka.common.log.remote.storage.{LogSegmentData, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}

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

  //FIXME: Remove
  def delegate(): RemoteStorageManager = {
    rsm
  }

  override def close(): Unit = {
    withClassLoader {
      rsm.close()
    }
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    withClassLoader {
      rsm.configure(configs)
    }
  }

  override def copyLogSegment(remoteLogSegmentId: RemoteLogSegmentId,
                              logSegmentData: LogSegmentData): Unit = {
    withClassLoader {
      rsm.copyLogSegment(remoteLogSegmentId, logSegmentData)
    }
  }

  override def fetchLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                                   startPosition: lang.Long,
                                   endPosition: lang.Long): InputStream = {
    withClassLoader {
      rsm.fetchLogSegmentData(remoteLogSegmentMetadata, startPosition, endPosition)
    }
  }

  override def fetchOffsetIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): InputStream = {
    withClassLoader {
      rsm.fetchOffsetIndex(remoteLogSegmentMetadata)
    }

  }

  override def fetchTimestampIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): InputStream = {
    withClassLoader {
      rsm.fetchTimestampIndex(remoteLogSegmentMetadata)
    }
  }

  override def deleteLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {
    withClassLoader {
      rsm.deleteLogSegment(remoteLogSegmentMetadata)
    }
  }

  override def fetchTransactionIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): InputStream = {
    withClassLoader {
      rsm.fetchTransactionIndex(remoteLogSegmentMetadata)
    }
  }
}
