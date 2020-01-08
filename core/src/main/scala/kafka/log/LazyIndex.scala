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

import LazyIndex._
import kafka.utils.CoreUtils.inLock
import kafka.utils.threadsafe

/**
  * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading (i.e. memory mapping) the
  * underlying index until it is accessed for the first time via the `get` method.
  *
  * This is an important optimization with regards to broker start-up time if it has a large number of segments.
  *
  * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses documentation
  * to establish their thread safety.
  *
  * @param loadIndex A function that takes a `File` pointing to an index and returns a loaded `AbstractIndex` instance.
  */
@threadsafe
class LazyIndex[T <: AbstractIndex] private (@volatile private var indexWrapper: IndexWrapper, loadIndex: File => T) {

  private val lock = new ReentrantLock()

  def file: File = indexWrapper.file

  def file_=(f: File): Unit = {
    inLock(lock) {
      indexWrapper.file = f
    }
  }

  def get: T = {
    indexWrapper match {
      case indexValue: IndexValue[T] => indexValue.index
      case _: IndexFile =>
        inLock(lock) {
          indexWrapper match {
            case indexValue: IndexValue[T] => indexValue.index
            case indexFile: IndexFile =>
              val indexValue = new IndexValue(loadIndex(indexFile.file))
              indexWrapper = indexValue
              indexValue.index
          }
        }
    }
  }

}

object LazyIndex {

  def forOffset(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[OffsetIndex] =
    new LazyIndex(new IndexFile(file), file => new OffsetIndex(file, baseOffset, maxIndexSize, writable))

  def forTime(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[TimeIndex] =
    new LazyIndex(new IndexFile(file), file => new TimeIndex(file, baseOffset, maxIndexSize, writable))

  private sealed trait IndexWrapper {
    def file: File
    def file_=(f: File)
  }

  private class IndexFile(@volatile var file: File) extends IndexWrapper

  private class IndexValue[T <: AbstractIndex](val index: T) extends IndexWrapper {
    override def file: File = index.file
    override def file_=(f: File): Unit = index.file = f
  }

}

