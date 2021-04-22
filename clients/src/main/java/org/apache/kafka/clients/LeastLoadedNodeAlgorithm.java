/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

/**
 * algorithms for selecting the least loaded node in a kafka cluster
 * from a client's point of view. used for selecting nodes to query for metadata
 * (for example)
 */
public enum LeastLoadedNodeAlgorithm {
  /**
   * default upstream kafka selection algorithm.
   * attempts to minimize latency, but may resuult in stickiness
   * and hammering of dedicated controllers and brokers down
   * for maintenance
   */
  VANILLA,
  /**
   * selects a random broker out of 3 candidates. candidates are preferrably
   * brokers with existing connections, but new connections will be initiated
   * to get the candidate pool up to 3.
   */
  AT_LEAST_THREE
}
