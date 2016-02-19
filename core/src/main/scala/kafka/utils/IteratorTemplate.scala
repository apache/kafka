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

class State
object DONE extends State
object READY extends State
object NOT_READY extends State
object FAILED extends State

/**
 * Transliteration of the iterator template in google collections. To implement an iterator
 * override makeNext and call allDone() when there is no more items
 */
abstract class IteratorTemplate[T] extends Iterator[T] with java.util.Iterator[T] {
  
  private var state: State = NOT_READY
  private var nextItem = null.asInstanceOf[T]

  def next(): T = {
    if(!hasNext())
      throw new NoSuchElementException()
    state = NOT_READY
    if(nextItem == null)
      throw new IllegalStateException("Expected item but none found.")
    nextItem
  }
  
  def peek(): T = {
    if(!hasNext())
      throw new NoSuchElementException()
    nextItem
  }
  
  def hasNext(): Boolean = {
    if(state == FAILED)
      throw new IllegalStateException("Iterator is in failed state")
    state match {
      case DONE => false
      case READY => true
      case _ => maybeComputeNext()
    }
  }
  
  protected def makeNext(): T
  
  def maybeComputeNext(): Boolean = {
    state = FAILED
    nextItem = makeNext()
    if(state == DONE) {
      false
    } else {
      state = READY
      true
    }
  }
  
  protected def allDone(): T = {
    state = DONE
    null.asInstanceOf[T]
  }
  
  def remove = 
    throw new UnsupportedOperationException("Removal not supported")

  protected def resetState() {
    state = NOT_READY
  }
}

