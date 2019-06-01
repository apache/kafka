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
package org.apache.kafka.streams.scala.kstream
import java.time.Duration

import org.apache.kafka.streams.kstream.{Windowed, Suppressed => SupressedJ}
import org.apache.kafka.streams.kstream.Suppressed.{
  EagerBufferConfig,
  StrictBufferConfig,
  BufferConfig => BufferConfigJ
}
import org.apache.kafka.streams.kstream.internals.suppress.{
  EagerBufferConfigImpl,
  FinalResultsSuppressionBuilder,
  StrictBufferConfigImpl,
  SuppressedInternal
}

/**
 * Duplicates the static factory methods inside the Java interface [[org.apache.kafka.streams.kstream.Suppressed]].
 *
 * This is required for compatibility w/ Scala 2.11 + Java 1.8 because the Scala 2.11 compiler doesn't support the use
 * of static methods inside Java interfaces.
 *
 * TODO: Deprecate this class if support for Scala 2.11 + Java 1.8 is dropped.
 */
object Suppressed {

  /**
   * Configure the suppression to emit only the "final results" from the window.
   *
   * By default all Streams operators emit results whenever new results are available.
   * This includes windowed operations.
   *
   * This configuration will instead emit just one result per key for each window, guaranteeing
   * to deliver only the final result. This option is suitable for use cases in which the business logic
   * requires a hard guarantee that only the final result is propagated. For example, sending alerts.
   *
   * To accomplish this, the operator will buffer events from the window until the window close (that is,
   * until the end-time passes, and additionally until the grace period expires). Since windowed operators
   * are required to reject late events for a window whose grace period is expired, there is an additional
   * guarantee that the final results emitted from this suppression will match any queriable state upstream.
   *
   * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
   *                     This is required to be a "strict" config, since it would violate the "final results"
   *                     property to emit early and then issue an update later.
   * @tparam K The [[Windowed]] key type for the KTable to apply this suppression to.
   * @return a "final results" mode suppression configuration
   * @see [[org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit]]
   */
  def untilWindowCloses[K](bufferConfig: StrictBufferConfig): SupressedJ[Windowed[K]] =
    new FinalResultsSuppressionBuilder[Windowed[K]](null, bufferConfig)

  /**
   * Configure the suppression to wait `timeToWaitForMoreEvents` amount of time after receiving a record
   * before emitting it further downstream. If another record for the same key arrives in the mean time, it replaces
   * the first record in the buffer but does <em>not</em> re-start the timer.
   *
   * @param timeToWaitForMoreEvents The amount of time to wait, per record, for new events.
   * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
   * @tparam K The key type for the KTable to apply this suppression to.
   * @return a suppression configuration
   * @see [[org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit]]
   */
  def untilTimeLimit[K](timeToWaitForMoreEvents: Duration, bufferConfig: BufferConfigJ[_]): SupressedJ[K] =
    new SuppressedInternal[K](null, timeToWaitForMoreEvents, bufferConfig, null, false)

  /**
   * Duplicates the static factory methods inside the Java interface
   * [[org.apache.kafka.streams.kstream.Suppressed.BufferConfig]].
   */
  object BufferConfig {

    /**
     * Create a size-constrained buffer in terms of the maximum number of keys it will store.
     *
     * @param recordLimit maximum number of keys to buffer.
     * @return size-constrained buffer in terms of the maximum number of keys it will store.
     * @see [[org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords]]
     */
    def maxRecords(recordLimit: Long): EagerBufferConfig =
      new EagerBufferConfigImpl(recordLimit, Long.MaxValue)

    /**
     * Create a size-constrained buffer in terms of the maximum number of bytes it will use.
     *
     * @param byteLimit maximum number of bytes to buffer.
     * @return size-constrained buffer in terms of the maximum number of bytes it will use.
     * @see [[org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes]]
     */
    def maxBytes(byteLimit: Long): EagerBufferConfig =
      new EagerBufferConfigImpl(Long.MaxValue, byteLimit)

    /**
     * Create a buffer unconstrained by size (either keys or bytes).
     *
     * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
     *
     * If there isn't enough heap available to meet the demand, the application will encounter an
     * [[OutOfMemoryError]] and shut down (not guaranteed to be a graceful exit). Also, note that
     * JVM processes under extreme memory pressure may exhibit poor GC behavior.
     *
     * This is a convenient option if you doubt that your buffer will be that large, but also don't
     * wish to pick particular constraints, such as in testing.
     *
     * This buffer is "strict" in the sense that it will enforce the time bound or crash.
     * It will never emit early.
     *
     * @return a buffer unconstrained by size (either keys or bytes).
     * @see [[org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded]]
     */
    def unbounded(): StrictBufferConfig = new StrictBufferConfigImpl()
  }
}
