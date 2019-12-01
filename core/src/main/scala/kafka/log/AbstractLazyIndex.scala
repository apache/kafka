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
abstract class AbstractLazyIndex[T <: AbstractIndex[_, _]](_file: File) {
  import AbstractLazyIndex._

  @volatile private var lazyIndex: LazyIndex = new UninitializedLazyIndex(_file)
  private val lock = new ReentrantLock()

  def file: File = lazyIndex.file

  def file_=(f: File): Unit = {
    inLock(lock) {
      lazyIndex.file = f
    }
  }

  def get: T = {
    lazyIndex match {
      case initializedIndex: InitializedLazyIndex[T] =>
        initializedIndex.index
      case _: UninitializedLazyIndex[T] =>
        inLock(lock) {
          lazyIndex match {
            case initializedIndex: InitializedLazyIndex[T] =>
              initializedIndex.index
            case uninitializedIndex: UninitializedLazyIndex[T] =>
              val initializedIndex = new InitializedLazyIndex(loadIndex(uninitializedIndex.indexFile))
              lazyIndex = initializedIndex
              initializedIndex.index
          }
        }
    }
  }

  /**
   * An abstract method that must be implemented by all subclasses in order to
   * construct a complete index object. Will be lazily invoked whenever index data
   * needs to be accessed.
   *
   * @param indexFile reference to a file object containing index data
   * @return instantiated index object
   */
  protected def loadIndex(indexFile: File): T

}

object AbstractLazyIndex {

  sealed trait LazyIndex {
    def file: File
    def file_=(f: File)
  }

  private class UninitializedLazyIndex[T <: AbstractIndex[_, _]](@volatile var indexFile: File) extends LazyIndex {
    override def file: File = indexFile
    override def file_=(f: File): Unit = indexFile = f
  }

  private class InitializedLazyIndex[T <: AbstractIndex[_, _]](val index: T) extends LazyIndex {
    override def file: File = index.file
    override def file_=(f: File): Unit = index.file = f
  }

}