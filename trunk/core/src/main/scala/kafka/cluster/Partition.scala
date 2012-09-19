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

package kafka.cluster

object Partition {
  def parse(s: String): Partition = {
    val pieces = s.split("-")
    if(pieces.length != 2)
      throw new IllegalArgumentException("Expected name in the form x-y.")
    new Partition(pieces(0).toInt, pieces(1).toInt)
  }
}

class Partition(val brokerId: Int, val partId: Int) extends Ordered[Partition] {

  def this(name: String) = {
    this(1, 1)
  }
  
  def name = brokerId + "-" + partId
  
  override def toString(): String = name

  def compare(that: Partition) =
    if (this.brokerId == that.brokerId)
      this.partId - that.partId
    else
      this.brokerId - that.brokerId

  override def equals(other: Any): Boolean = {
    other match {
      case that: Partition =>
        (that canEqual this) && brokerId == that.brokerId && partId == that.partId
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Partition]

  override def hashCode: Int = 31 * (17 + brokerId) + partId

}
