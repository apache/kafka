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

/**
 * Simple doubly LinkedList node
 * @param element The element
 * @tparam T The type of element
 */
class DoublyLinkedListNode[T] (val element: T) {
  var prev: DoublyLinkedListNode[T] = null
  var next: DoublyLinkedListNode[T] = null
}

/**
 * A simple doubly linked list util to allow O(1) remove.
 * @tparam T type of element in nodes
 */
@threadsafe
class DoublyLinkedList[T] {
  private var head: DoublyLinkedListNode[T] = null
  private var tail: DoublyLinkedListNode[T] = null
  @volatile private var listSize: Int = 0

  /**
   * Add offset to the tail of the list
   * @param node the node to be added to the tail of the list
   */
  def add (node: DoublyLinkedListNode[T]) {
    this synchronized {
      if (head == null) {
        // empty list
        head = node
        tail = node
        node.prev = null
        node.next = null
      } else {
        // add to tail
        tail.next = node
        node.next = null
        node.prev = tail
        tail = node
      }
      listSize += 1
    }
  }

  /**
   * Remove a node from the list. The list will not check if the node is really in the list.
   * @param node the node to be removed from the list
   */
  def remove (node: DoublyLinkedListNode[T]) {
    this synchronized {
      if (node ne head)
        node.prev.next = node.next
      else
        head = node.next

      if (node ne tail)
        node.next.prev = node.prev
      else
        tail = node.prev

      node.prev = null
      node.next = null

      listSize -= 1
    }
  }

  /**
   * Remove the first node in the list and return it if the list is not empty.
   * @return The first node in the list if the list is not empty. Return Null if the list is empty.
   */
  def remove(): DoublyLinkedListNode[T] = {
    this synchronized {
      if (head != null) {
        val node = head
        remove(head)
        node
      } else {
        null
      }
    }
  }

  /**
   * Get the first node in the list without removing it.
   * @return the first node in the list.
   */
  def peek(): DoublyLinkedListNode[T] = head

  def size: Int = listSize

  def iterator: Iterator[DoublyLinkedListNode[T]] = {
    new IteratorTemplate[DoublyLinkedListNode[T]] {
      var current = head
      override protected def makeNext(): DoublyLinkedListNode[T] = {
        this synchronized {
          if (current != null) {
            val nextNode = current
            current = current.next
            nextNode
          } else {
            allDone()
          }
        }
      }
    }
  }
}
