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

package org.apache.kafka.clients.tools;


/**
 * This class helps producers throttle their maximum message throughput.
 * 
 * The resulting average throughput will be approximately 
 * min(targetThroughput, maximumPossibleThroughput)
 * 
 * To use, do this between successive send attempts:
 * <pre>
 *     {@code     
 *      if (throttler.shouldThrottle(...)) {
 *          throttler.throttle();
 *      } 
 *     } 
 * </pre> 
 */
public class MessageThroughputThrottler {
    
    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    long sleepTimeNs;
    long sleepDeficitNs = 0;
    long targetThroughput = -1;
    long startMs;

    public MessageThroughputThrottler(long targetThroughput, long startMs) {
        this.startMs = startMs;
        this.targetThroughput = targetThroughput;
        this.sleepTimeNs = NS_PER_SEC / targetThroughput;
    }

    public boolean shouldThrottle(long messageNum, long sendStartMs) {
        if (this.targetThroughput <= 0) {
            // No throttling in this case
            return false;
        }

        float elapsedMs = (sendStartMs - startMs) / 1000.f;
        return elapsedMs > 0 && (messageNum / elapsedMs) > this.targetThroughput;
    }

    public void throttle() {
        // throttle message throughput by sleeping, on average,
        // (1 / this.throughput) seconds between each sent message
        sleepDeficitNs += sleepTimeNs;

        // If enough sleep deficit has accumulated, sleep a little
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepMs = sleepDeficitNs / 1000000;
            long sleepNs = sleepDeficitNs - sleepMs * 1000000;

            long sleepStartNs = System.nanoTime();
            try {
                Thread.sleep(sleepMs, (int) sleepNs);
                sleepDeficitNs = 0;
            } catch (InterruptedException e) {
                // If sleep is cut short, reduce deficit by the amount of
                // time we actually spent sleeping
                long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                if (sleepElapsedNs <= sleepDeficitNs) {
                    sleepDeficitNs -= sleepElapsedNs;
                }
            }
        }
    }
}
    
    