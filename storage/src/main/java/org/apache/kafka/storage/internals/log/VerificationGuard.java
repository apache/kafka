package org.apache.kafka.storage.internals.log;

import java.util.concurrent.atomic.AtomicLong;

public class VerificationGuard {
    private static final AtomicLong incrementingId = new AtomicLong(0L);
    private final long verificationGuardValue;

    public VerificationGuard() {
        verificationGuardValue = incrementingId.incrementAndGet();
    }

    @Override
    public String toString() {
        return "VerificationGuard: " + verificationGuardValue;
    }

    @Override
    public boolean equals(Object obj) {
    if ((null == obj) || (obj.getClass() != this.getClass()))
        return false;
    VerificationGuard guard = (VerificationGuard) obj;
    return verificationGuardValue == guard.verificationGuardValue();
    }

    @Override
    public int hashCode() {
        long value = verificationGuardValue;
        return (int)(value ^ (value >>> 32));
    }

    public long verificationGuardValue() {
        return verificationGuardValue;
    }
}
