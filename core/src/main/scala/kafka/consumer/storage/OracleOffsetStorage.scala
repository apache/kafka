/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.consumer.storage.sql

import java.sql._
import org.apache.log4j._
import kafka.utils._
import kafka.consumer.storage.OffsetStorage

/**
 * An offset storage implementation that uses an oracle database to save offsets
 */
@nonthreadsafe
class OracleOffsetStorage(val connection: Connection) extends OffsetStorage {
  
  private val logger: Logger = Logger.getLogger(classOf[OracleOffsetStorage])
  private val lock = new Object
  connection.setAutoCommit(false)
  
  def reserve(node: Int, topic: String): Long = {
    /* try to get and lock the offset, if it isn't there, make it */
    val maybeOffset = selectExistingOffset(connection, node, topic)
    val offset = maybeOffset match {
      case Some(offset) => offset
      case None => {
        maybeInsertZeroOffset(connection, node, topic)
        selectExistingOffset(connection, node, topic).get
      }
    }
    
    if(logger.isDebugEnabled)
      logger.debug("Reserved node " + node + " for topic '" + topic + " offset " + offset)
    
    offset
  }
  
  def commit(node: Int, topic: String, offset: Long) {
    var success = false
    try {
      updateOffset(connection, node, topic, offset)
      success = true
    } finally {
      commitOrRollback(connection, success)
    }
    if(logger.isDebugEnabled)
      logger.debug("Updated node " + node + " for topic '" + topic + "' to " + offset)
  }
  
  def close() {
    Utils.swallow(logger.error, connection.close())
  }
  
  /**
   * Attempt to update an existing entry in the table if there isn't already one there
   * @return true iff the row didn't already exist
   */
  private def maybeInsertZeroOffset(connection: Connection, node: Int, topic: String): Boolean = {
    val stmt = connection.prepareStatement(
      """insert into kafka_offsets (node, topic, offset) 
         select ?, ?, 0 from dual where not exists 
         (select null from kafka_offsets where node = ? and topic = ?)""")
    stmt.setInt(1, node)
    stmt.setString(2, topic)
    stmt.setInt(3, node)
    stmt.setString(4, topic)
    val updated = stmt.executeUpdate()
    if(updated > 1)
      throw new IllegalStateException("More than one key updated by primary key!")
    else
      updated == 1
  }
  
  /**
   * Attempt to update an existing entry in the table
   * @return true iff we updated an entry
   */
  private def selectExistingOffset(connection: Connection, node: Int, topic: String): Option[Long] = {
    val stmt = connection.prepareStatement(
        """select offset from kafka_offsets
           where node = ? and topic = ?
           for update""")
    var results: ResultSet = null
    try {
      stmt.setInt(1, node)
      stmt.setString(2, topic)
      results = stmt.executeQuery()
      if(!results.next()) {
        None
      } else {
        val offset = results.getLong("offset")
        if(results.next())
          throw new IllegalStateException("More than one entry for primary key!")
        Some(offset)
      }
    } finally {
      close(stmt)
      close(results)
    }
  }
  
  private def updateOffset(connection: Connection, 
                           node: Int, 
                           topic: String, 
                           newOffset: Long): Unit = {
    val stmt = connection.prepareStatement("update kafka_offsets set offset = ? where node = ? and topic = ?")
    try {
      stmt.setLong(1, newOffset)
      stmt.setInt(2, node)
      stmt.setString(3, topic)
      val updated = stmt.executeUpdate()
      if(updated != 1)
        throw new IllegalStateException("Unexpected number of keys updated: " + updated)
    } finally {
      close(stmt)
    }
  }
  
  
  private def commitOrRollback(connection: Connection, commit: Boolean) {
    if(connection != null) {
      if(commit)
        Utils.swallow(logger.error, connection.commit())
      else
        Utils.swallow(logger.error, connection.rollback())
    }
  }
  
  private def close(rs: ResultSet) {
    if(rs != null)
      Utils.swallow(logger.error, rs.close())
  }
  
  private def close(stmt: PreparedStatement) {
    if(stmt != null)
      Utils.swallow(logger.error, stmt.close())
  }
  
  private def close(connection: Connection) {
    if(connection != null)
      Utils.swallow(logger.error, connection.close())
  }
  
}
