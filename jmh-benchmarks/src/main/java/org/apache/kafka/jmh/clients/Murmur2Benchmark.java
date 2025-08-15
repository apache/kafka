package org.apache.kafka.jmh.clients;

import org.apache.kafka.common.utils.Utils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Murmur2Benchmark {

    private static final int SAMPLE_SIZE = 1000;

    public enum TestCase {
        TEST_CASE_1_4(1, 4, 0xcc6444ca02edfbd0L),
        TEST_CASE_1_16(1, 16, 0x187c616cabc3e0a7L),
        TEST_CASE_1_64(1, 64, 0xa820ddbf8f76273dL),
        TEST_CASE_1_256(1, 256, 0x898e4c60ab901376L),
        TEST_CASE_4_4(4, 4, 0x91c73b19f97f9e07L),
        TEST_CASE_16_16(16, 16, 0xae0ddf78c2705d4eL),
        TEST_CASE_64_64(64, 64, 0x9024a9c2f7355dcdL),
        TEST_CASE_256_256(256, 256, 0x548b0ff34eb8e4aaL);

        TestCase(int minLen, int maxLen, long seed) {
            data = createRandomByteArrays(SAMPLE_SIZE, minLen, maxLen, seed);
        }

        byte[][] data;
    }

    private static byte[][] createRandomByteArrays(int size, int minLen, int maxLen, long seed) {
        byte[][] result = new byte[size][];
        SplittableRandom random = new SplittableRandom(seed);
        for (int i = 0; i < size; ++i) {
            result[i] = createRandomByteArray(minLen, maxLen, random);
        }
        return result;
    }

    private static byte[] createRandomByteArray(int minLen, int maxLen, SplittableRandom random) {
        int len = random.nextInt(minLen, maxLen + 1);
        byte[] b = new byte[len];
        random.nextBytes(b);
        return b;
    }

    @State(Scope.Benchmark)
    public static class TestCaseState {
        @Param
        public TestCase testCase;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
    @Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
    @Fork(value = 1, warmups = 0)
    public void hashBytes(TestCaseState testCaseState, Blackhole blackhole) {
        var data = testCaseState.testCase.data;
        for (byte[] b : data) {
            blackhole.consume(Utils.murmur2(b));
        }
    }

}
