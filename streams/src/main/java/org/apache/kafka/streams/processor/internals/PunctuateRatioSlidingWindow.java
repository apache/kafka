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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Time;

import java.util.LinkedList;
import java.util.Queue;

public class PunctuateRatioSlidingWindow {
    private final Queue<RatioTimeStamp> ratioQueue;
    private final long windowSizeMillis;
    private final Time time;

    public PunctuateRatioSlidingWindow(long windowSizeMillis, Time time) {
        this.windowSizeMillis = windowSizeMillis;
        this.ratioQueue = new LinkedList<>();
        this.time=time;
    }

    public void update(double ratio){
        long currentTimeMillis = time.milliseconds();
        ratioQueue.offer(new RatioTimeStamp(ratio, currentTimeMillis));;
        pruneQueue(currentTimeMillis);
    }

    private void pruneQueue(long currentTimeMillis) {
        while(!ratioQueue.isEmpty()){
            RatioTimeStamp oldest = ratioQueue.peek();
            if(currentTimeMillis - oldest.getTimestamp() > windowSizeMillis) {
                ratioQueue.poll();
            } else {
                break;
            }
        }
    }

    public double getAverageRatio() {
        return ratioQueue.stream()
                .mapToDouble(RatioTimeStamp::getRatio)
                .average()
                .orElse(0.0);
    }
 }
