/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.SystemTime;

public class Microbenchmarks {

    public static void main(String[] args) throws Exception {

        final int iters = Integer.parseInt(args[0]);
        double x = 0.0;
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++)
            x += Math.sqrt(x);
        System.out.println(x);
        System.out.println("sqrt: " + (System.nanoTime() - start) / (double) iters);

        // test clocks
        systemMillis(iters);
        systemNanos(iters);
        long total = 0;
        start = System.nanoTime();
        total += systemMillis(iters);
        System.out.println("System.currentTimeMillis(): " + (System.nanoTime() - start) / iters);
        start = System.nanoTime();
        total += systemNanos(iters);
        System.out.println("System.nanoTime(): " + (System.nanoTime() - start) / iters);
        System.out.println(total);

        // test random
        int n = 0;
        Random random = new Random();
        start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            n += random.nextInt();
        }
        System.out.println(n);
        System.out.println("random: " + (System.nanoTime() - start) / iters);

        float[] floats = new float[1024];
        for (int i = 0; i < floats.length; i++)
            floats[i] = random.nextFloat();
        Arrays.sort(floats);

        int loc = 0;
        start = System.nanoTime();
        for (int i = 0; i < iters; i++)
            loc += Arrays.binarySearch(floats, floats[i % floats.length]);
        System.out.println(loc);
        System.out.println("binary search: " + (System.nanoTime() - start) / iters);

        final SystemTime time = new SystemTime();
        final AtomicBoolean done = new AtomicBoolean(false);
        final Object lock = new Object();
        Thread t1 = new Thread() {
            public void run() {
                time.sleep(1);
                int counter = 0;
                long start = time.nanoseconds();
                for (int i = 0; i < iters; i++) {
                    synchronized (lock) {
                        counter++;
                    }
                }
                System.out.println("synchronized: " + ((time.nanoseconds() - start) / iters));
                System.out.println(counter);
                done.set(true);
            }
        };

        Thread t2 = new Thread() {
            public void run() {
                int counter = 0;
                while (!done.get()) {
                    time.sleep(1);
                    synchronized (lock) {
                        counter += 1;
                    }
                }
                System.out.println("Counter: " + counter);
            }
        };

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        System.out.println("Testing locks");
        done.set(false);
        final ReentrantLock lock2 = new ReentrantLock();
        Thread t3 = new Thread() {
            public void run() {
                time.sleep(1);
                int counter = 0;
                long start = time.nanoseconds();
                for (int i = 0; i < iters; i++) {
                    lock2.lock();
                    counter++;
                    lock2.unlock();
                }
                System.out.println("lock: " + ((time.nanoseconds() - start) / iters));
                System.out.println(counter);
                done.set(true);
            }
        };

        Thread t4 = new Thread() {
            public void run() {
                int counter = 0;
                while (!done.get()) {
                    time.sleep(1);
                    lock2.lock();
                    counter++;
                    lock2.unlock();
                }
                System.out.println("Counter: " + counter);
            }
        };

        t3.start();
        t4.start();
        t3.join();
        t4.join();

        Map<String, Integer> values = new HashMap<String, Integer>();
        for (int i = 0; i < 100; i++)
            values.put(Integer.toString(i), i);
        System.out.println("HashMap:");
        benchMap(2, 1000000, values);
        System.out.println("ConcurentHashMap:");
        benchMap(2, 1000000, new ConcurrentHashMap<String, Integer>(values));
        System.out.println("CopyOnWriteMap:");
        benchMap(2, 1000000, new CopyOnWriteMap<String, Integer>(values));
    }

    private static void benchMap(int numThreads, final int iters, final Map<String, Integer> map) throws Exception {
        final List<String> keys = new ArrayList<String>(map.keySet());
        final List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    long start = System.nanoTime();
                    for (int j = 0; j < iters; j++)
                        map.get(keys.get(j % threads.size()));
                    System.out.println("Map access time: " + ((System.nanoTime() - start) / (double) iters));
                }
            });
        }
        for (Thread thread : threads)
            thread.start();
        for (Thread thread : threads)
            thread.join();
    }

    private static long systemMillis(int iters) {
        long total = 0;
        for (int i = 0; i < iters; i++)
            total += System.currentTimeMillis();
        return total;
    }

    private static long systemNanos(int iters) {
        long total = 0;
        for (int i = 0; i < iters; i++)
            total += System.currentTimeMillis();
        return total;
    }

}
