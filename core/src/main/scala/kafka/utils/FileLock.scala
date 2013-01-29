package kafka.utils

import java.io._
import java.nio.channels._

/**
 * A file lock a la flock/funlock
 * 
 * The given path will be created and opened if it doesn't exist.
 */
class FileLock(val file: File) extends Logging {
  file.createNewFile() // create the file if it doesn't exist
  private val channel = new RandomAccessFile(file, "rw").getChannel()
  private var flock: java.nio.channels.FileLock = null

  /**
   * Lock the file or throw an exception if the lock is already held
   */
  def lock() {
    this synchronized {
      trace("Acquiring lock on " + file.getAbsolutePath)
      flock = channel.lock()
    }
  }

  /**
   * Try to lock the file and return true if the locking succeeds
   */
  def tryLock(): Boolean = {
    this synchronized {
      trace("Acquiring lock on " + file.getAbsolutePath)
      try {
        // weirdly this method will return null if the lock is held by another
        // process, but will throw an exception if the lock is held by this process
        // so we have to handle both cases
        flock = channel.tryLock()
        flock != null
      } catch {
        case e: OverlappingFileLockException => false
      }
    }
  }

  /**
   * Unlock the lock if it is held
   */
  def unlock() {
    this synchronized {
      trace("Releasing lock on " + file.getAbsolutePath)
      if(flock != null)
        flock.release()
    }
  }

  /**
   * Destroy this lock, closing the associated FileChannel
   */
  def destroy() = {
    this synchronized {
      unlock()
      channel.close()
    }
  }
}