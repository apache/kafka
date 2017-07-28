
package kafka.tools;

public class Pacer {
        private long initialStartTime;
        private double throughputInUnitsPerNsec;
        private long unitsCompleted;

        private boolean caughtUp = true;
        private long catchUpStartTime;
        private long unitsCompletedAtCatchUpStart;
        private double catchUpThroughputInUnitsPerNsec;
        private double catchUpRateMultiple;

        public Pacer(double unitsPerSec) {
            this(unitsPerSec, 3.0); // Default to catching up at 3x the set throughput
        }

        public Pacer(double unitsPerSec, double catchUpRateMultiple) {
            setThroughout(unitsPerSec);
            setCatchupRateMultiple(catchUpRateMultiple);
            initialStartTime = System.nanoTime();
        }

        public void setInitialStartTime(long initialStartTime) {
            this.initialStartTime = initialStartTime;
        }

        public void setThroughout(double unitsPerSec) {
            throughputInUnitsPerNsec = unitsPerSec / 1000000000.0;
            catchUpThroughputInUnitsPerNsec = catchUpRateMultiple * throughputInUnitsPerNsec;
        }

        public void setCatchupRateMultiple(double multiple) {
            catchUpRateMultiple = multiple;
            catchUpThroughputInUnitsPerNsec = catchUpRateMultiple * throughputInUnitsPerNsec;
        }

        public long getUnitsCompleted(){
            return this.unitsCompleted;
        }

        /**
         * @return the time for the next operation
         */
        public long expectedStartTimeNsec() {
            return initialStartTime + (long)(unitsCompleted / throughputInUnitsPerNsec);
        }

        public long nsecToNextSend() {

            long now = System.nanoTime();

            long nextStartTime = expectedStartTimeNsec();

            boolean sendNow = true;

            if (nextStartTime > now) {
                // We are on pace. Indicate caught_up and don't send now.}
                caughtUp = true;
                sendNow = false;
            } else {
                // We are behind
                if (caughtUp) {
                    // This is the first fall-behind since we were last caught up
                    caughtUp = false;
                    catchUpStartTime = now;
                    unitsCompletedAtCatchUpStart = unitsCompleted;
                }

                // Figure out if it's time to send, per catch up throughput:
                long unitsCompletedSinceCatchUpStart =
                        unitsCompleted - unitsCompletedAtCatchUpStart;

                nextStartTime = catchUpStartTime +
                        (long)(unitsCompletedSinceCatchUpStart / catchUpThroughputInUnitsPerNsec);

                if (nextStartTime > now) {
                    // Not yet time to send, even at catch-up throughout:
                    sendNow = false;
                }
            }

            return sendNow ? 0 : (nextStartTime - now);
        }

        /**
         * Will wait for next operation time. After this the expectedStartTimeNsec() will move forward.
         * @param unitCount
         */
        public void acquire(long unitCount) {
            long nsecToNextSend = nsecToNextSend();
            if (nsecToNextSend > 0) {
                Timer.sleepNs(nsecToNextSend);
            }
            unitsCompleted += unitCount;
        }
    }