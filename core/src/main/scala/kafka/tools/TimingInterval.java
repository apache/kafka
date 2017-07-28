package kafka.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.HdrHistogram.Histogram;

// represents measurements taken over an interval of time
// used for both single timer results and merged timer results
public final class TimingInterval
{
    // nanos
    private final long start;
    private final long end;
    public final long pauseLength;
    public final long pauseStart;

    // discrete
    public final long partitionCount;
    public final long rowCount;
    public final long operationCount;

    public final Histogram responseTimesHistogram;
    public final Histogram serviceTimesHistogram;


    TimingInterval(long time)
    {
        start = end = time;
        partitionCount = rowCount = operationCount = 0;
        pauseStart = pauseLength = 0;
        responseTimesHistogram = new Histogram(3);
        serviceTimesHistogram = new Histogram(3);
    }

    TimingInterval(long start, long end, long pauseStart, long pauseLength, long partitionCount, long rowCount, long operationCount, Histogram responseTimesHistogram, Histogram serviceTimesHistogram)
    {
        this.start = start;
        this.end = Math.max(end, start);
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.operationCount = operationCount;
        this.pauseStart = pauseStart;
        this.pauseLength = pauseLength;
        this.serviceTimesHistogram = serviceTimesHistogram;
        this.responseTimesHistogram = responseTimesHistogram;
    }

    // merge multiple timer intervals together
    static TimingInterval merge(List<TimingInterval> intervals, int maxSamples, long start)
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long operationCount = 0, partitionCount = 0, rowCount = 0;
        long maxLatency = 0, totalLatency = 0;
        List<SampleOfLongs> latencies = new ArrayList<>();
        Histogram mergedResponseTimesHistogram = new Histogram(3);
        Histogram mergedServiceTimesHistogram = new Histogram(3);

        long startTime = Long.MAX_VALUE;
        long endTime = 0;

        long end = 0;
        long pauseStart = 0, pauseEnd = Long.MAX_VALUE;
        for (TimingInterval interval : intervals)
        {
            end = Math.max(end, interval.end);
            operationCount += interval.operationCount;
            partitionCount += interval.partitionCount;
            rowCount += interval.rowCount;
            mergedResponseTimesHistogram.add(interval.responseTimesHistogram);
            mergedServiceTimesHistogram.add(interval.serviceTimesHistogram);

            // track start and end time across all interval histograms:
            startTime = Math.min(startTime, interval.responseTimesHistogram.getStartTimeStamp());
            startTime = Math.min(startTime, interval.serviceTimesHistogram.getStartTimeStamp());
            endTime = Math.max(endTime, interval.responseTimesHistogram.getEndTimeStamp());
            endTime = Math.max(endTime, interval.serviceTimesHistogram.getEndTimeStamp());

            if (interval.pauseLength > 0)
            {
                pauseStart = Math.max(pauseStart, interval.pauseStart);
                pauseEnd = Math.min(pauseEnd, interval.pauseStart + interval.pauseLength);
            }
        }

        mergedServiceTimesHistogram.setStartTimeStamp(startTime);
        mergedServiceTimesHistogram.setEndTimeStamp(endTime);
        mergedResponseTimesHistogram.setStartTimeStamp(startTime);
        mergedResponseTimesHistogram.setEndTimeStamp(endTime);

        if (pauseEnd < pauseStart)
            pauseEnd = pauseStart = 0;

        return new TimingInterval(start, end, pauseStart, pauseEnd - pauseStart, partitionCount, rowCount, operationCount,
                mergedResponseTimesHistogram, mergedServiceTimesHistogram);

    }

    public double opRate()
    {
        return operationCount / ((end - start) * 0.000000001d);
    }

    public double adjustedRowRate()
    {
        return rowCount / ((end - (start + pauseLength)) * 0.000000001d);
    }

    public double partitionRate()
    {
        return partitionCount / ((end - start) * 0.000000001d);
    }

    public double rowRate()
    {
        return rowCount / ((end - start) * 0.000000001d);
    }

    public double meanLatency()
    {
        return responseTimesHistogram.getMean() * 0.000001d;
    }

    public double serviceTimesMeanLatency()
    {
        return serviceTimesHistogram.getMean() * 0.000001d;
    }

    public double maxLatency()
    {
        return responseTimesHistogram.getMaxValue() * 0.000001d;
    }

    public double serviceTimesMaxLatency()
    {
        return serviceTimesHistogram.getMaxValue() * 0.000001d;
    }

    public long runTime()
    {
        return (end - start) / 1000000;
    }

    public double medianLatency()
    {
        return responseTimesHistogram.getValueAtPercentile(50.0) * 0.000001d;
    }

    public double serviceTimesMedianLatency()
    {
        return serviceTimesHistogram.getValueAtPercentile(50.0) * 0.000001d;
    }

    // 0 < rank < 1
    public double rankLatency(float rank)
    {
        return responseTimesHistogram.getValueAtPercentile(rank * 100.0) * 0.000001d;
    }

    public double serviceTimesRankLatency(float rank)
    {
        return serviceTimesHistogram.getValueAtPercentile(rank * 100.0) * 0.000001d;
    }

    public final long endNanos()
    {
        return end;
    }

    public final long endMillis()
    {
        return end / 1000000;
    }

    public long startNanos()
    {
        return start;
    }

    public Histogram getResponseTimesHistogram() {
        return responseTimesHistogram;
    }

    public Histogram getServiceTimesHistogram() {
        return serviceTimesHistogram;
    }
}

