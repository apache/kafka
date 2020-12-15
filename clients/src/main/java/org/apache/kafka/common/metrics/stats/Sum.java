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
package org.apache.kafka.common.metrics.stats;

/**
 * A {@link SampledStat} that maintains the sum of what it has seen.
 * This is a sampled version of {@link CumulativeSum}.
 *
 * See also {@link WindowedCount} if you want to increment the value by 1 on each recording.
 *
 * @deprecated since 2.4 . Use {@link WindowedSum} instead
 */
@Deprecated
public class Sum extends WindowedSum {
}
