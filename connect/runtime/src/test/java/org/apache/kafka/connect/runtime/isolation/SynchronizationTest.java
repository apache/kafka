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

    @Test(timeout = 15000L)
    // If the test times out, then there's a deadlock in the test but not necessarily the code
    public void workerContextClassLoaderMismatch() throws Exception {
        // Grab a reference to the target PluginClassLoader before activating breakpoints
        ClassLoader connectorLoader = plugins.delegatingLoader()
            .connectorLoader(TestPlugins.SAMPLING_CONVERTER);

        // Simulate Worker::startConnector that creates configs with the delegating classloader
        Runnable delegatingToPlugin = () -> {
            // Use the DelegatingClassLoader as the current context loader
            ClassLoader savedLoader = Plugins.compareAndSwapLoaders(plugins.delegatingLoader());

            // Load an isolated plugin from the delegating classloader, which will
            // 1. Lock the DelegatingClassLoader (via Class.forName)
            // 2. Wait for test to continue
            // 3. Attempt to lock the PluginClassLoader (via PluginClassLoader::loadClass)
            // 4. Deadlock
            new AbstractConfig(
                new ConfigDef().define("a.class", Type.CLASS, Importance.HIGH, ""),
                Collections.singletonMap("a.class", TestPlugins.SAMPLING_CONVERTER));
            Plugins.compareAndSwapLoaders(savedLoader);
        };
        // DelegatingClassLoader breakpoint will only trigger on this thread
        dclBreakpoint.set(TestPlugins.SAMPLING_CONVERTER::equals);

        // Simulate Worker::startTask that creates configs with the plugin classloader
        Runnable pluginToDelegating = () -> {
            // Use the PluginClassLoader as the current context loader
            ClassLoader savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);
            // Load a non-isolated class from the plugin classloader, which will
            // 1. Lock the PluginClassLoader (via PluginClassLoader::loadClass)
            // 2. Wait for the test to continue
            // 3. Attempt to lock the DelegatingClassLoader (via ClassLoader::loadClass)
            // 4. Deadlock
            new AbstractConfig(new ConfigDef().define("a.class", Type.CLASS, Importance.HIGH, ""),
                Collections.singletonMap("a.class", "org.apache.kafka.connect.storage.JsonConverter"));
            Plugins.compareAndSwapLoaders(savedLoader);
        };
        // PluginClassLoader breakpoint will only trigger on this thread
        pclBreakpoint.set("org.apache.kafka.connect.storage.JsonConverter"::equals);

        // Step 1: Lock the delegating classloader and pause
        exec.submit(delegatingToPlugin);
        // d2p enters ConfigDef::parseType
        // d2p enters DelegatingClassLoader::loadClass
        dclBreakpoint.testAwait();
        dclBreakpoint.testAwait();
        // d2p exits DelegatingClassLoader::loadClass
        // d2p enters Class::forName
        // d2p LOCKS DelegatingClassLoader
        // d2p enters DelegatingClassLoader::loadClass
        dclBreakpoint.testAwait();
        // d2p waits in the delegating classloader while we set up the other thread
        dumpThreads("d2p waiting with DelegatingClassLoader locked");

        // Step 2: Lock the plugin classloader and then the delegating classloader
        exec.submit(pluginToDelegating);
        // p2d enters PluginClassLoader::loadClass
        // p2d LOCKS PluginClassLoader
        pclBreakpoint.testAwait();
        // p2d locks the class-specific classLoadingLock
        // p2d falls through to ClassLoader::loadClass
        pclBreakpoint.testAwait();
        // p2d locks the class-specific classLoadingLock
        // p2d delegates upwards to DelegatingClassLoader::loadClass
        // p2d enters ClassLoader::loadClass
        // p2d LOCKS DelegatingClassLoader (starting the deadlock)
        dumpThreads("p2d blocked trying to acquire the DelegatingClassLoader lock");

        // Step 3: Resume the first thread and try to lock the plugin classloader
        dclBreakpoint.testAwait();
        // d2p enters PluginClassLoader::loadClass
        // d2p LOCKS PluginClassLoader (completing the deadlock)
        dumpThreads("d2p blocked trying to acquire the PluginClassLoader lock");
        assertNoDeadlocks();
    }

    // This is an informative test that is supposed to fail, demonstrating that forName is
    // locking the connectorLoader in this JVM implementation, when initialize is true.
    @Test
    public void forNameLocksClassloader()
        throws InterruptedException, TimeoutException, BrokenBarrierException {
        // It is not important here that we're using the PluginClassLoader.
        // This behavior is specific to the JVM, not the classloader implementation
        // It is just a convenient classloader instance that we can throw away after the test
        ClassLoader connectorLoader = plugins.delegatingLoader()
            .connectorLoader(TestPlugins.SAMPLING_CONVERTER);

        Object monitor = new Object();
        Breakpoint<Object> monitorBreakpoint = new Breakpoint<>();
        Breakpoint<Object> progress = new Breakpoint<>();

        Runnable executeForName = () -> {
            synchronized (monitor) {
                try {
                    progress.await(null);
                    Class.forName(TestPlugins.SAMPLING_CONVERTER, true, connectorLoader);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to load test plugin", e);
                }
            }
        };
        progress.set(null);

        Runnable holdsMonitorLock = () -> {
            synchronized (connectorLoader) {
                monitorBreakpoint.await(null);
                monitorBreakpoint.await(null);
                synchronized (monitor) {
                }
            }
        };
        monitorBreakpoint.set(null);

        exec.submit(holdsMonitorLock);
        // LOCK the classloader
        // wait for test to progress
        dumpThreads("locked the classloader");
        monitorBreakpoint.testAwait();

        exec.submit(executeForName);
        progress.testAwait();
        // LOCK the monitor
        // LOCK the classloader (starting the deadlock)
        dumpThreads("half-deadlocked");
        monitorBreakpoint.testAwait();
        // LOCK the monitor (completing the deadlock)

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

    private void dumpThreads(String msg) throws InterruptedException {
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
        //for (; i < stackTrace.length && i < MAX_FRAMES; i++) {
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
        // This also becomes non-functional
        //if (i < stackTrace.length) {
        //sb.append("\t...");
        //sb.append('\n');
        //}
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
