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
import java.nio.file.{Files, NoSuchFileException}
import java.util.concurrent.locks.ReentrantLock

import LazyIndex._
import kafka.utils.CoreUtils.inLock
import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

/**
  * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading
  * (i.e. memory mapping) the underlying index until it is accessed for the first time via the
  * `get` method.
  *
  * In addition, this class exposes a number of methods (e.g. updateParentDir, renameTo, close,
  * etc.) that provide the desired behavior without causing the index to be loaded. If the index
  * had previously been loaded, the methods in this class simply delegate to the relevant method in
  * the index.
  *
  * This is an important optimization with regards to broker start-up and shutdown time if it has a
  * large number of segments.
  *
  * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses
  * documentation to establish their thread safety.
  *
  * @param loadIndex A function that takes a `File` pointing to an index and returns a loaded
  *                  `AbstractIndex` instance.
  */
@threadsafe
class LazyIndex[T <: AbstractIndex] private (@volatile private var indexWrapper: IndexWrapper, loadIndex: File => T) {

  private val lock = new ReentrantLock()

  def file: File = indexWrapper.file

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

  def updateParentDir(parentDir: File): Unit = {
    inLock(lock) {
      indexWrapper.updateParentDir(parentDir)
    }
  }

  def renameTo(f: File): Unit = {
    inLock(lock) {
      indexWrapper.renameTo(f)
    }
  }

  def deleteIfExists(): Boolean = {
    inLock(lock) {
      indexWrapper.deleteIfExists()
    }
  }

  def close(): Unit = {
    inLock(lock) {
      indexWrapper.close()
    }
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      indexWrapper.closeHandler()
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

    def updateParentDir(f: File): Unit

    def renameTo(f: File): Unit

    def deleteIfExists(): Boolean

    def close(): Unit

    def closeHandler(): Unit

  }

  private class IndexFile(@volatile private var _file: File) extends IndexWrapper {

    def file: File = _file

    def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

    def renameTo(f: File): Unit = {
      try Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
      catch {
        case _: NoSuchFileException if !file.exists => ()
      }
      finally _file = f
    }

    def deleteIfExists(): Boolean = Files.deleteIfExists(file.toPath)

    def close(): Unit = ()

    def closeHandler(): Unit = ()

  }

  private class IndexValue[T <: AbstractIndex](val index: T) extends IndexWrapper {

    def file: File = index.file

    def updateParentDir(parentDir: File): Unit = index.updateParentDir(parentDir)

    def renameTo(f: File): Unit = index.renameTo(f)

    def deleteIfExists(): Boolean = index.deleteIfExists()

    def close(): Unit = index.close()

    def closeHandler(): Unit = index.closeHandler()

  }

}

