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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.apache.kafka.streams.kstream.internals.emitstrategy.WindowCloseStrategy;
import org.apache.kafka.streams.kstream.internals.emitstrategy.WindowUpdateStrategy;

/**
 * This interface controls the strategy that can be used to control how we emit results in a processor.
 */
public interface EmitStrategy {

    enum StrategyType {
        ON_WINDOW_CLOSE,
        ON_WINDOW_UPDATE
    }

    /**
     * Returns the strategy type
     * @return Emit strategy type
     */
    StrategyType type();

    /**
     * This strategy indicates that the aggregated result for a window will only be emitted when the
     * window closes instead of when there's an update to the window. Window close means that current
     * event time is larger than (window end time + grace period).
     *
     * <p>This strategy should only be used for windows which can close. An exception will be thrown
     * if it's used with {@link UnlimitedWindow}.
     *
     * @see TimeWindows
     * @see SlidingWindows
     * @see SessionWindows
     * @see UnlimitedWindows
     * @see WindowUpdateStrategy
     *
     * @return WindowCloseStrategy instance
     */
    static EmitStrategy onWindowClose() {
        return new WindowCloseStrategy();
    }

    /**
     * This strategy indicates that the aggregated result for a window will be emitted every time
     * when there's an update to the window instead of when the window closes.
     *
     * @see TimeWindows
     * @see SlidingWindows
     * @see SessionWindows
     * @see UnlimitedWindows
     * @see WindowCloseStrategy
     *
     * @return WindowCloseStrategy instance
     */
    static EmitStrategy onWindowUpdate() {
        return new WindowUpdateStrategy();
    }
}