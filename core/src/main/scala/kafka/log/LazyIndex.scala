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

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.concurrent.locks.ReentrantLock

import LazyIndex._
import kafka.utils.CoreUtils.inLock
import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

/**
  * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading (i.e. memory mapping) the
  * underlying index until it is accessed for the first time via the `get` method.
  *
  * This is an important optimization with regards to broker start-up and shutdown time if it has a large number of segments.
  * It prevents illegal accesses to the underlying index after closing the index, which might otherwise lead to memory
  * leaks due to recreation of underlying memory mapped object.
  *
  * Finally, this wrapper ensures that redundant disk accesses and memory mapped operations are avoided upon attempts to
  * delete or rename the file that backs this index.
  *
  * @param loadIndex A function that takes a `File` pointing to an index and returns a loaded `AbstractIndex` instance.
  */
@threadsafe
class LazyIndex[T <: AbstractIndex] private (@volatile private var indexWrapper: IndexWrapper, loadIndex: File => T) {
  // A closed index does not allow accessing its indices to prevent side effects.
  @volatile private var isClosed: Boolean = false
  private val lock = new ReentrantLock()

  def file: File = indexWrapper.file

  def file_=(f: File): Unit = {
    inLock(lock) {
      indexWrapper.file = f
    }
  }

  def get: T = {
    if (isClosed)
      throw new IllegalStateException(s"Attempt to access the closed Index (file=$file).")
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

  /**
   * Close this index file.
   * Note: This will be a no-op if the index has already been closed.
   */
  def close(): Unit = {
    if (!isClosed) {
      inLock(lock) {
        indexWrapper match {
          case indexValue: IndexValue[T] => indexValue.index.close()
          case _: IndexFile => // no-op
        }
      }
      isClosed = true
    }
  }

  /**
   * Delete the index file that backs this index if exists.
   * This method ensures that if the index file has already been closed or in case it has not been created before,
   * it will not be recreated as a side effect.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    if (isClosed)
      Files.deleteIfExists(file.toPath)
    else {
      inLock(lock) {
        indexWrapper match {
          case indexValue: IndexValue[T] => indexValue.index.deleteIfExists()
          case _: IndexFile => Files.deleteIfExists(file.toPath)
        }
      }
    }
  }

  /**
   * Rename the file that backs this index if the index has ever been initialized or file already exists.
   *
   * @throws IOException if rename fails for defined index or existing file.
   */
  def renameTo(f: File) {
    try {
      if (file.exists)
        Utils.atomicMoveWithFallback(file.toPath, f.toPath)
      else {
        inLock(lock) {
          indexWrapper match {
            case indexValue: IndexValue[T] => Utils.atomicMoveWithFallback(file.toPath, f.toPath)
            case _: IndexFile => // no-op
          }
        }
      }
    } finally file = f
  }
}

object LazyIndex {

  def forOffset(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[OffsetIndex] =
    new LazyIndex(new IndexFile(file), file => new OffsetIndex(file, baseOffset, maxIndexSize, writable))

  def forTime(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[TimeIndex] =
    new LazyIndex(new IndexFile(file), file => new TimeIndex(file, baseOffset, maxIndexSize, writable))

  private sealed trait IndexWrapper {
    def file: File
    def file_=(f: File): Unit
  }

  private class IndexFile(@volatile var file: File) extends IndexWrapper

  private class IndexValue[T <: AbstractIndex](val index: T) extends IndexWrapper {
    override def file: File = index.file
    override def file_=(f: File): Unit = index.file = f
  }

}

