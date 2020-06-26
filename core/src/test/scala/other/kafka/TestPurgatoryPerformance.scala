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

package kafka

import java.lang.management.ManagementFactory
import java.lang.management.OperatingSystemMXBean
import java.util.Random
import java.util.concurrent._

import joptsimple._
import kafka.server.{DelayedOperation, DelayedOperationPurgatory}
import kafka.utils._
import org.apache.kafka.common.utils.Time

import scala.math._
import scala.jdk.CollectionConverters._

/**
 * This is a benchmark test of the purgatory.
 */
object TestPurgatoryPerformance {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val keySpaceSizeOpt = parser.accepts("key-space-size", "The total number of possible keys")
      .withRequiredArg
      .describedAs("total_num_possible_keys")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val numRequestsOpt = parser.accepts("num", "The number of requests")
      .withRequiredArg
      .describedAs("num_requests")
      .ofType(classOf[java.lang.Double])
    val requestRateOpt = parser.accepts("rate", "The request rate per second")
      .withRequiredArg
      .describedAs("request_per_second")
      .ofType(classOf[java.lang.Double])
    val requestDataSizeOpt = parser.accepts("size", "The request data size in bytes")
      .withRequiredArg
      .describedAs("num_bytes")
      .ofType(classOf[java.lang.Long])
    val numKeysOpt = parser.accepts("keys", "The number of keys for each request")
      .withRequiredArg
      .describedAs("num_keys")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val timeoutOpt = parser.accepts("timeout", "The request timeout in ms")
      .withRequiredArg
      .describedAs("timeout_milliseconds")
      .ofType(classOf[java.lang.Long])
    val pct75Opt = parser.accepts("pct75", "75th percentile of request latency in ms (log-normal distribution)")
      .withRequiredArg
      .describedAs("75th_percentile")
      .ofType(classOf[java.lang.Double])
    val pct50Opt = parser.accepts("pct50", "50th percentile of request latency in ms (log-normal distribution)")
      .withRequiredArg
      .describedAs("50th_percentile")
      .ofType(classOf[java.lang.Double])
    val verboseOpt = parser.accepts("verbose", "show additional information")
      .withRequiredArg
      .describedAs("true|false")
      .ofType(classOf[java.lang.Boolean])
      .defaultsTo(true)

    val options = parser.parse(args: _*)

    CommandLineUtils.checkRequiredArgs(parser, options, numRequestsOpt, requestRateOpt, requestDataSizeOpt, pct75Opt, pct50Opt)

    val numRequests = options.valueOf(numRequestsOpt).intValue
    val requestRate = options.valueOf(requestRateOpt).doubleValue
    val requestDataSize = options.valueOf(requestDataSizeOpt).intValue
    val numPossibleKeys = options.valueOf(keySpaceSizeOpt).intValue
    val numKeys = options.valueOf(numKeysOpt).intValue
    val timeout = options.valueOf(timeoutOpt).longValue
    val pct75 = options.valueOf(pct75Opt).doubleValue
    val pct50 = options.valueOf(pct50Opt).doubleValue
    val verbose = options.valueOf(verboseOpt).booleanValue

    val gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans().asScala.sortBy(_.getName)
    val osMXBean = ManagementFactory.getOperatingSystemMXBean
    val latencySamples = new LatencySamples(1000000, pct75, pct50)
    val intervalSamples = new IntervalSamples(1000000, requestRate)

    val purgatory = DelayedOperationPurgatory[FakeOperation]("fake purgatory")
    val queue = new CompletionQueue()

    val gcNames = gcMXBeans.map(_.getName)

    val initialCpuTimeNano = getProcessCpuTimeNanos(osMXBean)
    val latch = new CountDownLatch(numRequests)
    val start = System.currentTimeMillis
    val rand = new Random()
    val keys = (0 until numKeys).map(_ => "fakeKey%d".format(rand.nextInt(numPossibleKeys)))
    @volatile var requestArrivalTime = start
    @volatile var end = 0L
    val generator = new Runnable {
      def run(): Unit = {
        var i = numRequests
        while (i > 0) {
          i -= 1
          val requestArrivalInterval = intervalSamples.next()
          val latencyToComplete = latencySamples.next()
          val now = System.currentTimeMillis
          requestArrivalTime = requestArrivalTime + requestArrivalInterval

          if (requestArrivalTime > now) Thread.sleep(requestArrivalTime - now)

          val request = new FakeOperation(timeout, requestDataSize, latencyToComplete, latch)
          if (latencyToComplete < timeout) queue.add(request)
          purgatory.tryCompleteElseWatch(request, keys)
        }
        end = System.currentTimeMillis
      }
    }
    val generatorThread = new Thread(generator)

    generatorThread.start()
    generatorThread.join()
    latch.await()
    val done = System.currentTimeMillis
    queue.shutdown()

    if (verbose) {
      latencySamples.printStats()
      intervalSamples.printStats()
      println("# enqueue rate (%d requests):".format(numRequests))
      val gcCountHeader = gcNames.map("<" + _ + " count>").mkString(" ")
      val gcTimeHeader = gcNames.map("<" + _ + " time ms>").mkString(" ")
      println("# <elapsed time ms>\t<target rate>\t<actual rate>\t<process cpu time ms>\t%s\t%s".format(gcCountHeader, gcTimeHeader))
    }

    val targetRate = numRequests.toDouble * 1000d / (requestArrivalTime - start).toDouble
    val actualRate = numRequests.toDouble * 1000d / (end - start).toDouble

    val cpuTime = getProcessCpuTimeNanos(osMXBean).map(x => (x - initialCpuTimeNano.get) / 1000000L)
    val gcCounts = gcMXBeans.map(_.getCollectionCount)
    val gcTimes = gcMXBeans.map(_.getCollectionTime)

    println("%d\t%f\t%f\t%d\t%s\t%s".format(done - start, targetRate, actualRate, cpuTime.getOrElse(-1L), gcCounts.mkString(" "), gcTimes.mkString(" ")))

    purgatory.shutdown()
  }

  // Use JRE-specific class to get process CPU time
  private def getProcessCpuTimeNanos(osMXBean : OperatingSystemMXBean) = {
    try {
      Some(Class.forName("com.sun.management.OperatingSystemMXBean").getMethod("getProcessCpuTime").invoke(osMXBean).asInstanceOf[Long])
    } catch {
      case _: Throwable => try {
        Some(Class.forName("com.ibm.lang.management.OperatingSystemMXBean").getMethod("getProcessCpuTimeByNS").invoke(osMXBean).asInstanceOf[Long])
      } catch {
        case _: Throwable => None
      }
    }
  }

  // log-normal distribution (http://en.wikipedia.org/wiki/Log-normal_distribution)
  //   mu: the mean of the underlying normal distribution (not the mean of this log-normal distribution)
  //   sigma: the standard deviation of the underlying normal distribution (not the stdev of this log-normal distribution)
  private class LogNormalDistribution(mu: Double, sigma: Double) {
    val rand = new Random
    def next(): Double = {
      val n = rand.nextGaussian() * sigma + mu
      math.exp(n)
    }
  }

  // exponential distribution (http://en.wikipedia.org/wiki/Exponential_distribution)
  //  lambda : the rate parameter of the exponential distribution
  private class ExponentialDistribution(lambda: Double) {
    val rand = new Random
    def next(): Double = {
      math.log(1d - rand.nextDouble()) / (- lambda)
    }
  }

  // Samples of Latencies to completion
  // They are drawn from a log normal distribution.
  // A latency value can never be negative. A log-normal distribution is a convenient way to
  // model such a random variable.
  private class LatencySamples(sampleSize: Int, pct75: Double, pct50: Double) {
    private[this] val rand = new Random
    private[this] val samples = {
      val normalMean = math.log(pct50)
      val normalStDev = (math.log(pct75) - normalMean) / 0.674490d // 0.674490 is 75th percentile point in N(0,1)
      val dist = new LogNormalDistribution(normalMean, normalStDev)
      (0 until sampleSize).map { _ => dist.next().toLong }.toArray
    }
    def next() = samples(rand.nextInt(sampleSize))

    def printStats(): Unit = {
      val p75 = samples.sorted.apply((sampleSize.toDouble * 0.75d).toInt)
      val p50 = samples.sorted.apply((sampleSize.toDouble * 0.5d).toInt)

      println("# latency samples: pct75 = %d, pct50 = %d, min = %d, max = %d".format(p75, p50, samples.min, samples.max))
    }
  }

  // Samples of Request arrival intervals
  // The request arrival is modeled as a Poisson process.
  // So, the internals are drawn from an exponential distribution.
  private class IntervalSamples(sampleSize: Int, requestPerSecond: Double) {
    private[this] val rand = new Random
    private[this] val samples = {
      val dist = new ExponentialDistribution(requestPerSecond / 1000d)
      var residue = 0.0
      (0 until sampleSize).map { _ =>
        val interval = dist.next() + residue
        val roundedInterval = interval.toLong
        residue = interval - roundedInterval.toDouble
        roundedInterval
      }.toArray
    }

    def next() = samples(rand.nextInt(sampleSize))

    def printStats(): Unit = {
      println(
        "# interval samples: rate = %f, min = %d, max = %d"
          .format(1000d / (samples.map(_.toDouble).sum / sampleSize.toDouble), samples.min, samples.max)
      )
    }
  }

  private class FakeOperation(delayMs: Long, size: Int, val latencyMs: Long, latch: CountDownLatch) extends DelayedOperation(delayMs) {
    val completesAt = System.currentTimeMillis + latencyMs

    def onExpiration(): Unit = {}

    def onComplete(): Unit = {
      latch.countDown()
    }

    def tryComplete(): Boolean = {
      if (System.currentTimeMillis >= completesAt)
        forceComplete()
      else
        false
    }
  }

  private class CompletionQueue {
    private[this] val delayQueue = new DelayQueue[Scheduled]()
    private[this] val thread = new ShutdownableThread(name = "completion thread", isInterruptible = false) {
      override def doWork(): Unit = {
        val scheduled = delayQueue.poll(100, TimeUnit.MILLISECONDS)
        if (scheduled != null) {
          scheduled.operation.forceComplete()
        }
      }
    }
    thread.start()

    def add(operation: FakeOperation): Unit = {
      delayQueue.offer(new Scheduled(operation))
    }

    def shutdown() = {
      thread.shutdown()
    }

    private class Scheduled(val operation: FakeOperation) extends Delayed {
      def getDelay(unit: TimeUnit): Long = {
        unit.convert(max(operation.completesAt - Time.SYSTEM.milliseconds, 0), TimeUnit.MILLISECONDS)
      }

      def compareTo(d: Delayed): Int = {

        val other = d.asInstanceOf[Scheduled]

        if (operation.completesAt < other.operation.completesAt) -1
        else if (operation.completesAt > other.operation.completesAt) 1
        else 0
      }
    }
  }
}
