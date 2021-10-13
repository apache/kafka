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
package kafka.server.metadata

import org.apache.kafka.image.MetadataImage


/**
 * Handles creating snapshots.
 */
trait MetadataSnapshotter {
  /**
   * If there is no other snapshot being written out, start writing out a snapshot.
   *
   * @param lastContainedLogTime  The highest time contained in the snapshot.
   * @param image                 The metadata image to write out.
   *
   * @return                      True if we will write out a new snapshot; false otherwise.
   */
  def maybeStartSnapshot(lastContainedLogTime: Long, image: MetadataImage): Boolean
}
