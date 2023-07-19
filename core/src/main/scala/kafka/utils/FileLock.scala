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
 package kafka.utils

import java.io._
import java.nio.channels._
import java.nio.file.StandardOpenOption

/**
 * A file lock a la flock/funlock
 * 
 * The given path will be created and opened if it doesn't exist.
 */
class FileLock(val file: File) extends Logging {

  private val channel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
    StandardOpenOption.WRITE)
  private var flock: java.nio.channels.FileLock = _

  /**
   * Lock the file or throw an exception if the lock is already held
   */
  def lock(): Unit = {
    this synchronized {
      trace(s"Acquiring lock on ${file.getAbsolutePath}")
      flock = channel.lock()
    }
  }

  /**
   * Try to lock the file and return true if the locking succeeds
   */
  def tryLock(): Boolean = {
    this synchronized {
      trace(s"Acquiring lock on ${file.getAbsolutePath}")
      try {
        // weirdly this method will return null if the lock is held by another
        // process, but will throw an exception if the lock is held by this process
        // so we have to handle both cases
        flock = channel.tryLock()
        flock != null
      } catch {
        case _: OverlappingFileLockException => false
      }
    }
  }

  /**
   * Unlock the lock if it is held
   */
  def unlock(): Unit = {
    this synchronized {
      trace(s"Releasing lock on ${file.getAbsolutePath}")
      if(flock != null)
        flock.release()
    }
  }

  /**
   * Destroy this lock, closing the associated FileChannel
   */
  def destroy(): Unit = {
    this synchronized {
      unlock()
      channel.close()
    }
  }
}
