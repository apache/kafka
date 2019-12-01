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

package kafka.log

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock

/**
 * An abstract lazy index wrapper that provides capability of avoiding index initialization
 * on construction.
 *
 * This defers the index initialization to the time it gets accessed
 * so the cost of the heavy memory mapped operation gets amortized over time.
 */
abstract class AbstractLazyIndex[T <: AbstractIndex[_, _]](private val _file: File) {

  @volatile private var lazyIndex: LazyIndex = UninitializedLazyIndex(_file)
  private val lock = new ReentrantLock()

  sealed trait LazyIndex {
    def file: File
  }

  case class UninitializedLazyIndex(indexFile: File) extends LazyIndex {
    override def file: File = indexFile
  }

  case class InitializedLazyIndex(index: T) extends LazyIndex {
    override def file: File = index.file
  }

  def file: File = lazyIndex.file

  def file_=(f: File): Unit = {
    inLock(lock) {
      val localLazyIndex = lazyIndex
      localLazyIndex match {
        case initializedIndex: InitializedLazyIndex =>
          initializedIndex.index.file = f
        case _: UninitializedLazyIndex =>
          lazyIndex = UninitializedLazyIndex(f)
      }
    }
  }

  def get: T = {
    var localLazyIndex = lazyIndex
    localLazyIndex match {
      case initializedIndex: InitializedLazyIndex =>
        initializedIndex.index
      case _: UninitializedLazyIndex =>
        inLock(lock) {
          localLazyIndex = lazyIndex
          localLazyIndex match {
            case initializedIndex: InitializedLazyIndex =>
              initializedIndex.index
            case uninitializedIndex: UninitializedLazyIndex =>
              val initializedIndex = InitializedLazyIndex(initializeIndex(uninitializedIndex.indexFile))
              lazyIndex = initializedIndex
              initializedIndex.index
          }
        }
    }
  }

  def initializeIndex(indexFile: File): T

}
