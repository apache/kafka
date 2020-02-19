/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.utils;

import java.util.concurrent.ThreadLocalRandom;

public class Jitter {
  /**
   * Adds 1% to a value. If value is 0, returns 0. If value is negative, adds 1% of abs(value) to it
   *
   * @param value Number to add 1% to. Could be negative.
   * @return Value +1% of abs(value)
   */
  public static long addOnePercentJitter(long value) {
    if (value == 0 || value < 100) {
      return value;
    }
    return value + ThreadLocalRandom.current().nextInt((int) Math.abs(value) / 100);
  }
}
