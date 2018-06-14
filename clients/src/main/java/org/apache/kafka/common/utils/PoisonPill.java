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
package org.apache.kafka.common.utils;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;


public class PoisonPill {

    public static void die() {
        die(null, -1, 1);
    }

    public static void die(File heapDumpFolder, final long maxWaitForDump) {
        die(heapDumpFolder, maxWaitForDump, 1);
    }

    public static void die(File heapDumpFolder, final long maxWaitForDump, int haltStatusCode) {
        try {
            if (maxWaitForDump > 0 && heapDumpFolder != null) {
                grabHeapDump(heapDumpFolder, maxWaitForDump, haltStatusCode);
            }
        } catch (Exception e) {
            System.err.println("unable to complete heap dump");
            e.printStackTrace(System.err);
            System.err.flush();
        } finally {
            Runtime.getRuntime().halt(haltStatusCode);
        }
    }

    private static void grabHeapDump(File heapDumpFolder, final long maxWait, final int haltStatusCode) throws Exception {

        //set up a watchdog background thread that will halt in ~maxWait regardless of whether or not
        //we succeed in taking a heap dump (since we dont know when it'll ever complete)
        final CountDownLatch latch = new CountDownLatch(1);
        Thread watchdog = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.countDown();
                    Thread.sleep(maxWait);
                    //at this point ~maxWait has passed since the call to die().
                    //if the heap dump process completed successfully die() would
                    //have called halt() and we wouldnt be here (99.99%)
                    System.err.println("heap dump (probably) did not complete within timeout. halting.");
                    System.err.flush();
                } catch (Exception e) {
                    System.err.println("watchdog caught exception");
                    e.printStackTrace(System.err);
                    System.err.flush();
                } finally {
                    Runtime.getRuntime().halt(haltStatusCode);
                }
            }
        });
        watchdog.setDaemon(true);
        watchdog.setName("clark the death watchdog");
        watchdog.start();

        //make sure the watchdog is up and running before we go off attempting to dump
        if (!latch.await(maxWait, TimeUnit.MILLISECONDS)) {
            System.err.println("unable to start watchdog within timeout. will not proceed with dump");
            System.err.flush();
            return;
        }

        System.err.println("dumping heap to " + heapDumpFolder.getCanonicalPath());
        System.err.flush();

        //we dump into dump.inprogress and atomically rename it to be dump.complete
        //(overwriting any previous such file). this attempts to guarantee there are
        //at most 2 (potentially large) dump files at any point in time.

        File inProgress = new File(heapDumpFolder, "dump.inprogress");
        File complete = new File(heapDumpFolder, "dump.complete");
        if (inProgress.exists() && !inProgress.delete()) {
            System.err.println("unable to delete existing dump file. will not proceed with dump");
            System.err.flush();
            return;
        }

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        HotSpotDiagnosticMXBean diagnosticMBean =
            ManagementFactory.newPlatformMXBeanProxy(server, "com.sun.management:type=HotSpotDiagnostic",
                HotSpotDiagnosticMXBean.class);
        diagnosticMBean.dumpHeap(inProgress.getCanonicalPath(), false /* disable only live - dump all objects */);
        Files.move(inProgress.toPath(), complete.toPath(), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
    }
}
