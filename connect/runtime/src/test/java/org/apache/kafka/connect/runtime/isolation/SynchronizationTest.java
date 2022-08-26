/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.runtime.isolation;

import static org.junit.Assert.fail;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronizationTest {

    public static final Logger log = LoggerFactory.getLogger(SynchronizationTest.class);

    @Rule
    public final TestName testName = new TestName();

    private String threadPrefix;
    private Plugins plugins;
    private ThreadPoolExecutor exec;
    private Breakpoint<String> dclBreakpoint;
    private Breakpoint<String> pclBreakpoint;

    @Before
    public void setup() {
        TestPlugins.assertAvailable();
        Map<String, String> pluginProps = Collections.singletonMap(
            WorkerConfig.PLUGIN_PATH_CONFIG,
            String.join(",", TestPlugins.pluginPath())
        );
        threadPrefix = SynchronizationTest.class.getSimpleName()
            + "." + testName.getMethodName() + "-";
        dclBreakpoint = new Breakpoint<>();
        pclBreakpoint = new Breakpoint<>();
        plugins = new Plugins(pluginProps) {
            @Override
            protected DelegatingClassLoader newDelegatingClassLoader(List<String> paths) {
                return AccessController.doPrivileged(
                    (PrivilegedAction<DelegatingClassLoader>) () ->
                        new SynchronizedDelegatingClassLoader(paths)
                );
            }
        };
        exec = new ThreadPoolExecutor(
            2,
            2,
            1000L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(),
            threadFactoryWithNamedThreads(threadPrefix)
        );

    }

    @After
    public void tearDown() throws InterruptedException {
        dclBreakpoint.clear();
        pclBreakpoint.clear();
        exec.shutdown();
        exec.awaitTermination(1L, TimeUnit.SECONDS);
    }

    private static class Breakpoint<T> {

        private Predicate<T> predicate;
        private CyclicBarrier barrier;

        public synchronized void clear() {
            if (barrier != null) {
                barrier.reset();
            }
            predicate = null;
            barrier = null;
        }

        public synchronized void set(Predicate<T> predicate) {
            clear();
            this.predicate = predicate;
            // As soon as the barrier is tripped, the barrier will be reset for the next round.
            barrier = new CyclicBarrier(2);
        }

        /**
         * From a thread under test, await for the test orchestrator to continue execution
         * @param obj Object to test with the breakpoint's current predicate
         */
        public void await(T obj) {
            Predicate<T> predicate;
            CyclicBarrier barrier;
            synchronized (this) {
                predicate  = this.predicate;
                barrier = this.barrier;
            }
            if (predicate != null && !predicate.test(obj)) {
                return;
            }
            if (barrier != null) {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException("Interrupted while waiting for load gate", e);
                }
            }
        }

        /**
         * From the test orchestrating thread, await for the test thread to continue execution
         * @throws InterruptedException If the current thread is interrupted while waiting
         * @throws BrokenBarrierException If the test thread is interrupted while waiting
         * @throws TimeoutException If the barrier is not reached before 1s passes.
         */
        public void testAwait()
            throws InterruptedException, BrokenBarrierException, TimeoutException {
            CyclicBarrier barrier;
            synchronized (this) {
                barrier = this.barrier;
            }
            Objects.requireNonNull(barrier, "Barrier must be set up before awaiting");
            barrier.await(1L, TimeUnit.SECONDS);
        }
    }

    private class SynchronizedDelegatingClassLoader extends DelegatingClassLoader {
        {
            ClassLoader.registerAsParallelCapable();
        }

        public SynchronizedDelegatingClassLoader(List<String> pluginPaths) {
            super(pluginPaths);
        }

        @Override
        protected PluginClassLoader newPluginClassLoader(
            URL pluginLocation,
            URL[] urls,
            ClassLoader parent
        ) {
            return AccessController.doPrivileged(
                (PrivilegedAction<PluginClassLoader>) () ->
                    new SynchronizedPluginClassLoader(pluginLocation, urls, parent)
            );
        }

        @Override
        public PluginClassLoader pluginClassLoader(String name) {
            dclBreakpoint.await(name);
            dclBreakpoint.await(name);
            return super.pluginClassLoader(name);
        }
    }

    private class SynchronizedPluginClassLoader extends PluginClassLoader {
        {
            ClassLoader.registerAsParallelCapable();
        }


        public SynchronizedPluginClassLoader(URL pluginLocation, URL[] urls, ClassLoader parent) {
            super(pluginLocation, urls, parent);
        }

        @Override
        protected Object getClassLoadingLock(String className) {
            pclBreakpoint.await(className);
            return super.getClassLoadingLock(className);
        }
    }

    // If the test times out, then there's a deadlock in the test but not necessarily the code
    @Test(timeout = 15000L)
    public void testSimultaneousUpwardAndDownwardDelegating() throws Exception {
        String t1Class = TestPlugins.SAMPLING_CONVERTER;
        // Grab a reference to the target PluginClassLoader before activating breakpoints
        ClassLoader connectorLoader = plugins.delegatingLoader().connectorLoader(t1Class);

        // THREAD 1: loads a class by delegating downward starting from the DelegatingClassLoader
        // DelegatingClassLoader breakpoint will only trigger on this thread
        dclBreakpoint.set(t1Class::equals);
        Runnable thread1 = () -> {
            // Use the DelegatingClassLoader as the current context loader
            ClassLoader savedLoader = Plugins.compareAndSwapLoaders(plugins.delegatingLoader());

            // Load an isolated plugin from the delegating classloader, which will
            // 1. Enter the DelegatingClassLoader
            // 2. Wait on dclBreakpoint for test to continue
            // 3. Enter the PluginClassLoader
            // 4. Load the isolated plugin class and return
            new AbstractConfig(
                new ConfigDef().define("a.class", Type.CLASS, Importance.HIGH, ""),
                Collections.singletonMap("a.class", t1Class));
            Plugins.compareAndSwapLoaders(savedLoader);
        };

        // THREAD 2: loads a class by delegating upward starting from the PluginClassLoader
        String t2Class = JsonConverter.class.getName();
        // PluginClassLoader breakpoint will only trigger on this thread
        pclBreakpoint.set(t2Class::equals);
        Runnable thread2 = () -> {
            // Use the PluginClassLoader as the current context loader
            ClassLoader savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);
            // Load a non-isolated class from the plugin classloader, which will
            // 1. Enter the PluginClassLoader
            // 2. Wait for the test to continue
            // 3. Enter the DelegatingClassLoader
            // 4. Load the non-isolated class and return
            new AbstractConfig(new ConfigDef().define("a.class", Type.CLASS, Importance.HIGH, ""),
                Collections.singletonMap("a.class", t2Class));
            Plugins.compareAndSwapLoaders(savedLoader);
        };

        // STEP 1: Have T1 enter the DelegatingClassLoader and pause
        exec.submit(thread1);
        // T1 enters ConfigDef::parseType
        // T1 enters DelegatingClassLoader::loadClass
        dclBreakpoint.testAwait();
        dclBreakpoint.testAwait();
        // T1 exits DelegatingClassLoader::loadClass
        // T1 enters Class::forName
        // T1 enters DelegatingClassLoader::loadClass
        dclBreakpoint.testAwait();
        // T1 waits in the delegating classloader while we set up the other thread
        dumpThreads("step 1, T1 waiting in DelegatingClassLoader");

        // STEP 2: Have T2 enter PluginClassLoader, delegate upward to the Delegating classloader
        exec.submit(thread2);
        // T2 enters PluginClassLoader::loadClass
        pclBreakpoint.testAwait();
        // T2 falls through to ClassLoader::loadClass
        pclBreakpoint.testAwait();
        // T2 delegates upwards to DelegatingClassLoader::loadClass
        // T2 enters ClassLoader::loadClass and loads the class from the parent (CLASSPATH)
        dumpThreads("step 2, T2 entered DelegatingClassLoader and is loading class from parent");

        // STEP 3: Resume T1 and have it enter the PluginClassLoader
        dclBreakpoint.testAwait();
        // T1 enters PluginClassLoader::loadClass
        dumpThreads("step 3, T1 entered PluginClassLoader and is/was loading class from isolated jar");

        // If the DelegatingClassLoader and PluginClassLoader are both not parallel capable, then this test will deadlock
        // Otherwise, T1 should be able to complete it's load from the PluginClassLoader concurrently with T2,
        // before releasing the DelegatingClassLoader and allowing T2 to complete.
        // As the DelegatingClassLoader is not parallel capable, it must be the case that PluginClassLoader is.
        assertNoDeadlocks();
    }

    // If the test times out, then there's a deadlock in the test but not necessarily the code
    @Test(timeout = 15000L)
    // Ensure the PluginClassLoader is parallel capable and not synchronized on its monitor lock
    public void testPluginClassLoaderDoesntHoldMonitorLock()
        throws InterruptedException, TimeoutException, BrokenBarrierException {
        String t1Class = TestPlugins.SAMPLING_CONVERTER;
        ClassLoader connectorLoader = plugins.delegatingLoader().connectorLoader(t1Class);

        Object externalTestLock = new Object();
        Breakpoint<Object> testBreakpoint = new Breakpoint<>();
        Breakpoint<Object> progress = new Breakpoint<>();

        // THREAD 1: hold the PluginClassLoader's monitor lock, and attempt to grab the external lock
        testBreakpoint.set(null);
        Runnable thread1 = () -> {
            synchronized (connectorLoader) {
                testBreakpoint.await(null);
                testBreakpoint.await(null);
                synchronized (externalTestLock) {
                }
            }
        };

        // THREAD 2: load a class via forName while holding some external lock
        progress.set(null);
        Runnable thread2 = () -> {
            synchronized (externalTestLock) {
                try {
                    progress.await(null);
                    Class.forName(TestPlugins.SAMPLING_CONVERTER, true, connectorLoader);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to load test plugin", e);
                }
            }
        };

        // STEP 1: Have T1 hold the PluginClassLoader's monitor lock
        exec.submit(thread1);
        // LOCK the classloader monitor lock
        testBreakpoint.testAwait();
        dumpThreads("step 1, T1 holding classloader monitor lock");

        // STEP 2: Have T2 hold the external lock, and proceed to perform classloading
        exec.submit(thread2);
        // LOCK the external lock
        progress.testAwait();
        // perform class loading
        dumpThreads("step 2, T2 holding external lock");

        // STEP 3: Have T1 grab the external lock, and then release both locks
        testBreakpoint.testAwait();
        // LOCK the external lock
        dumpThreads("step 3, T1 grabbed external lock");

        // If the PluginClassLoader was not parallel capable, then these threads should deadlock
        // Otherwise, classloading should proceed without grabbing the monitor lock, and complete before T1 grabs the external lock from T2.
        assertNoDeadlocks();
    }

    private boolean threadFromCurrentTest(ThreadInfo threadInfo) {
        return threadInfo.getThreadName().startsWith(threadPrefix);
    }

    private void assertNoDeadlocks() {
        long[] deadlockedThreads = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
        if (deadlockedThreads != null && deadlockedThreads.length > 0) {
            final String threads = Arrays
                .stream(ManagementFactory.getThreadMXBean().getThreadInfo(deadlockedThreads))
                .filter(this::threadFromCurrentTest)
                .map(SynchronizationTest::threadInfoToString)
                .collect(Collectors.joining(""));
            if (!threads.isEmpty()) {
                fail("Found deadlocked threads while classloading\n" + threads);
            }
        }
    }

    private void dumpThreads(String msg) {
        if (log.isDebugEnabled()) {
            log.debug("{}:\n{}",
                msg,
                Arrays.stream(ManagementFactory.getThreadMXBean().dumpAllThreads(true, true))
                    .filter(this::threadFromCurrentTest)
                    .map(SynchronizationTest::threadInfoToString)
                    .collect(Collectors.joining("\n"))
            );
        }
    }

    private static String threadInfoToString(ThreadInfo info) {
        StringBuilder sb = new StringBuilder("\"" + info.getThreadName() + "\"" +
            " Id=" + info.getThreadId() + " " +
            info.getThreadState());
        if (info.getLockName() != null) {
            sb.append(" on " + info.getLockName());
        }
        if (info.getLockOwnerName() != null) {
            sb.append(" owned by \"" + info.getLockOwnerName() +
                "\" Id=" + info.getLockOwnerId());
        }
        if (info.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (info.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        // this has been refactored for checkstyle
        printStacktrace(info, sb);
        LockInfo[] locks = info.getLockedSynchronizers();
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = " + locks.length);
            sb.append('\n');
            for (LockInfo li : locks) {
                sb.append("\t- " + li);
                sb.append('\n');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    private static void printStacktrace(ThreadInfo info, StringBuilder sb) {
        StackTraceElement[] stackTrace = info.getStackTrace();
        int i = 0;
        // This is a copy of ThreadInfo::toString but with an unlimited number of frames shown.
        for (; i < stackTrace.length; i++) {
            StackTraceElement ste = stackTrace[i];
            sb.append("\tat " + ste.toString());
            sb.append('\n');
            if (i == 0 && info.getLockInfo() != null) {
                Thread.State ts = info.getThreadState();
                switch (ts) {
                    case BLOCKED:
                        sb.append("\t-  blocked on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    case WAITING:
                        sb.append("\t-  waiting on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    case TIMED_WAITING:
                        sb.append("\t-  waiting on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : info.getLockedMonitors()) {
                if (mi.getLockedStackDepth() == i) {
                    sb.append("\t-  locked " + mi);
                    sb.append('\n');
                }
            }
        }
    }

    private static ThreadFactory threadFactoryWithNamedThreads(String threadPrefix) {
        AtomicInteger threadNumber = new AtomicInteger(1);
        return r -> {
            // This is essentially Executors.defaultThreadFactory except with
            // custom thread names so in order to filter by thread names when debugging
            SecurityManager s = System.getSecurityManager();
            Thread t = new Thread((s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup(), r,
                threadPrefix + threadNumber.getAndIncrement(),
                0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        };
    }

}
