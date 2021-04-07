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

package kafka.server

import org.apache.kafka.common.utils.BufferSupplier

object RequestLocal {
  val NoCaching: RequestLocal = RequestLocal(BufferSupplier.NO_CACHING)

  /** The returned instance should be confined to a single thread. */
  def withThreadConfinedCaching: RequestLocal = RequestLocal(BufferSupplier.create())
}

/**
 * Container for stateful instances where the lifecycle is scoped to one request.
 *
 * When each request is handled by one thread, efficient data structures with no locking or atomic operations
 * can be used (see RequestLocal.withThreadConfinedCaching).
 */
case class RequestLocal(bufferSupplier: BufferSupplier) {
  def close(): Unit = bufferSupplier.close()
}
