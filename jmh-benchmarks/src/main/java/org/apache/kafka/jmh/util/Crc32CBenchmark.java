package org.apache.kafka.jmh.util;

import org.apache.kafka.common.utils.Crc32C;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class Crc32CBenchmark {

    @Param({"false", "true"})
    private boolean direct;


    @Param({"false", "true"})
    private boolean readonly;

    @Param({"42"})
    private int seed;

    @Param({"128", "1024", "4096"})
    private int bytes;

    private ByteBuffer input;

    @Setup
    public void setup() {
        SplittableRandom random = new SplittableRandom(seed);
        input = direct ? ByteBuffer.allocateDirect(bytes) : ByteBuffer.allocate(bytes);
        for (int o = 0; o < bytes; o++) {
            input.put(o, (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1));
        }
        if (readonly) {
            input = input.asReadOnlyBuffer();
        }
    }

    @Benchmark
    public long checksum() {
        return Crc32C.compute(input, 0, bytes);
    }

}
