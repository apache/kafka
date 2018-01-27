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

package org.apache.kafka.testkit;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class MiniZookeeperNode implements AutoCloseable {
    private final static int TICK_TIME = 500;
    private final Logger log;
    private final String name;
    private File dir = null;
    private File logDir = null;
    private File snapshotDir = null;
    private ZooKeeperServer zkServer = null;
    private NIOServerCnxnFactory factory = null;
    private int port = -1;

    MiniZookeeperNode(MiniKafkaClusterBuilder clusterBld, String name) {
        this.log = new LogContext(clusterBld.logContext.logPrefix() + ": " + name + ": ").
            logger(MiniZookeeperNode.class);
        this.name = name;
    }

    /**
     * Returns the host:port combination in use, or null if ZK is not running.
     */
    public String hostPort() {
        if (port == -1) {
            return null;
        } else {
            return "localhost:" + port;
        }
    }

    /**
     * Start Zookeeper.  Throws an exception on failure.
     */
    public void start() {
        close();
        boolean success = false;
        try {
            this.dir = TestKitUtil.createTempDir(name + "-");
            this.snapshotDir = Files.createDirectory(new File(dir, "snapshot").toPath()).toFile();
            this.logDir = Files.createDirectory(new File(dir, "logs").toPath()).toFile();
            this.zkServer = new ZooKeeperServer(snapshotDir, logDir, TICK_TIME);
            this.factory = new NIOServerCnxnFactory();
            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
            factory.configure(addr, 0);
            factory.startup(zkServer);
            this.port = zkServer.getClientPort();
            success = true;
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (!success) {
                close();
            }
        }
    }

    /**
     * Stops Zookeeper.
     * Does not wait for shutdown to complete.
     * Does not throw exceptions.
     */
    public void shutdown() {
        if (factory != null) {
            try {
                factory.shutdown();
            } catch (Throwable e) {
                log.error("Error shutting down factory", e);
            } finally {
                factory = null;
            }
        }
        if (zkServer != null) {
            try {
                zkServer.shutdown();
            } catch (Throwable e) {
                log.error("Error shutting down zkServer", e);
            } finally {
                zkServer = null;
            }
        }
    }

    /**
     * Stops Zookeeper.
     * Waits for all threads to be stopped.
     * Does not throw exceptions.
     */
    @Override
    public void close() {
        if (this.port != -1) {
            while (true) {
                try {
                    sendFourLetterWord("stat", 500);
                } catch (IOException e) {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        shutdown();
        if (dir != null) {
            try {
                Utils.delete(dir);
            } catch (Throwable e) {
                log.error("Error deleting logDir", e);
            } finally {
                dir = null;
            }
        }
    }

    private void sendFourLetterWord(String word, int timeout) throws IOException {
        InetSocketAddress addr = new InetSocketAddress("localhost", port);
        try (Socket sock = new Socket()) {
            sock.connect(addr, timeout);
            OutputStream out = sock.getOutputStream();
            out.write(word.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }
}
