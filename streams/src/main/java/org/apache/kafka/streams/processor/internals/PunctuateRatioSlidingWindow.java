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
