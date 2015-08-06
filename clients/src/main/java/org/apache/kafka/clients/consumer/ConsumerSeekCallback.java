/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * A callback interface that the user can implement to trigger custom actions when a fetch request after a seek completes. The callback
 * may be executed in any thread calling {@link Consumer#poll(long) poll()}.
 */
public interface ConsumerSeekCallback {

  /**
   * A callback method the user can implement to provide asynchronous handling of seek request completion.
   * This method will be called when the fetch request sent to the server after a seek has been acknowledged.
   *
   * @param exception The exception thrown during processing of the request if the position to seek is out of range,
   *                  or null if the seek completed successfully
   */
  void onComplete(Exception exception);
}