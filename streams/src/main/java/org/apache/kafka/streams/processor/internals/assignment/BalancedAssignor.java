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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public interface BalancedAssignor<ID extends Comparable<? super ID>> {

    Map<ID, List<TaskId>> assign(final SortedSet<ID> clients,
                                 final SortedSet<TaskId> tasks,
                                 final Map<ID, Integer> clientsToNumberOfStreamThreads,
                                 final int balanceFactor);
}
