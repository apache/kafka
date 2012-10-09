package kafka.metrics

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

import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import java.util.concurrent.TimeUnit
import junit.framework.Assert._
import com.yammer.metrics.core.{MetricsRegistry, Clock}

class KafkaTimerTest extends JUnit3Suite {

  @Test
  def testKafkaTimer() {
    val clock = new ManualClock
    val testRegistry = new MetricsRegistry(clock)
    val metric = testRegistry.newTimer(this.getClass, "TestTimer")

    val timer = new KafkaTimer(metric)
    timer.time {
      clock.addMillis(1000)
    }
    assertEquals(1, metric.getCount())
    assertTrue((metric.getMax() - 1000).abs <= Double.Epsilon)
    assertTrue((metric.getMin() - 1000).abs <= Double.Epsilon)
  }

  private class ManualClock extends Clock {

    private var ticksInNanos = 0L

    override def getTick() = {
      ticksInNanos
    }

    override def getTime() = {
      TimeUnit.NANOSECONDS.toMillis(ticksInNanos)
    }

    def addMillis(millis: Long) {
      ticksInNanos += TimeUnit.MILLISECONDS.toNanos(millis)
    }
  }
}
