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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * A lightweight, client-agnostic Kafka wire-protocol fault-injection proxy for fast integration tests.
 *
 * <p>Sit it in front of a real (embedded) broker and point any client's {@code bootstrap.servers} at it;
 * it decodes requests/responses with Kafka's own protocol classes ({@link RequestHeader},
 * {@link AbstractResponse}, {@link RequestUtils#serialize}) — so it is correct across every wire version,
 * including flexible/tagged-field ones, with no hand-rolled byte offsets.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (var broker = new EmbeddedKafkaCluster(1)) {
 *     broker.start();
 *     try (var proxy = KafkaProtocolFaultProxy.inFrontOf(broker.bootstrapServers())) {
 *         // point clients here:
 *         props.put(BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
 *
 *         proxy.injectError(ApiKeys.END_TXN, Errors.CONCURRENT_TRANSACTIONS).once();
 *         proxy.injectError(ApiKeys.PRODUCE, Errors.NOT_ENOUGH_REPLICAS).onCall(2);
 *         proxy.disconnectOn(ApiKeys.END_TXN).once();      // the EOS "commit gap"
 *     }
 * }
 * }</pre>
 *
 * <p>Routing is transparent: the proxy rewrites {@code Metadata}/{@code FindCoordinator} responses so every
 * advertised address points back at itself, so a single-broker embedded cluster needs no special config
 * (its own ephemeral port is discovered from {@code bootstrapServers()}).
 *
 * <p>Determinism: {@code once()}/{@code onCall(n)}/{@code times(n)} are deterministic and safe for
 * assertions; {@code withProbability(p)} is chaos-mode only. The proxy never closes sockets unless a
 * {@code disconnectOn(...)} rule fires, so it is not itself a source of flakiness.
 */
public final class KafkaProtocolFaultProxy implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProtocolFaultProxy.class);

    /**
     * Per-API setter that stamps an injected {@link Errors} onto a decoded response. Only APIs registered
     * here support {@code injectError(...)}; anything else fails fast at rule registration.
     */
    private static final Map<ApiKeys, BiConsumer<AbstractResponse, Errors>> ERROR_SETTERS = new EnumMap<>(ApiKeys.class);
    static {
        ERROR_SETTERS.put(ApiKeys.END_TXN, (r, e) -> ((EndTxnResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.INIT_PRODUCER_ID, (r, e) -> ((InitProducerIdResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.ADD_OFFSETS_TO_TXN, (r, e) -> ((AddOffsetsToTxnResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.PRODUCE, (r, e) -> {
            final ProduceResponseData data = ((org.apache.kafka.common.requests.ProduceResponse) r).data();
            data.responses().forEach(topic ->
                topic.partitionResponses().forEach(p -> p.setErrorCode(e.code())));
        });
    }

    private final String targetHost;
    private final int targetPort;
    private final ExecutorService threadPool = Executors.newCachedThreadPool(r -> {
        final Thread t = new Thread(r, "kafka-fault-proxy");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<FaultRule> rules = new CopyOnWriteArrayList<>();
    private ServerSocket serverSocket;
    private volatile String proxyHost;
    private volatile int proxyPort;

    private KafkaProtocolFaultProxy(final String targetBootstrap) {
        final String hostPort = targetBootstrap.split(",")[0].trim();
        final int idx = hostPort.lastIndexOf(':');
        this.targetHost = hostPort.substring(0, idx);
        this.targetPort = Integer.parseInt(hostPort.substring(idx + 1));
    }

    /** Create and start a proxy in front of the given broker bootstrap address. */
    public static KafkaProtocolFaultProxy inFrontOf(final String targetBootstrap) {
        final KafkaProtocolFaultProxy proxy = new KafkaProtocolFaultProxy(targetBootstrap);
        try {
            proxy.start();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start fault proxy", e);
        }
        return proxy;
    }

    private void start() throws Exception {
        serverSocket = new ServerSocket(0);
        proxyPort = serverSocket.getLocalPort();
        proxyHost = "localhost";
        running.set(true);
        threadPool.submit(this::acceptLoop);
        LOG.info("Fault proxy listening on {}:{} -> broker {}:{}", proxyHost, proxyPort, targetHost, targetPort);
    }

    /** The address to hand to a client's {@code bootstrap.servers}. */
    public String bootstrapServers() {
        return proxyHost + ":" + proxyPort;
    }

    // ------------------------------------------------------------------
    // DSL
    // ------------------------------------------------------------------

    /** Rewrite responses of {@code apiKey} to carry {@code error}. Follow with a trigger, e.g. {@code .once()}. */
    public FaultRule.Builder injectError(final ApiKeys apiKey, final Errors error) {
        if (!ERROR_SETTERS.containsKey(apiKey)) {
            throw new IllegalArgumentException("Error injection is not supported for " + apiKey
                    + " yet. Supported: " + ERROR_SETTERS.keySet()
                    + " (add a setter to ERROR_SETTERS to extend).");
        }
        if (apiKey == ApiKeys.METADATA || apiKey == ApiKeys.FIND_COORDINATOR) {
            throw new IllegalArgumentException(apiKey + " is reserved for routing and cannot carry injected errors.");
        }
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.INJECT_ERROR, error);
    }

    /** Drop the connection when a response of {@code apiKey} would be returned (models the EOS commit gap). */
    public FaultRule.Builder disconnectOn(final ApiKeys apiKey) {
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.DISCONNECT, null);
    }

    /** Remove all registered faults (routing rewrites are always on and unaffected). */
    public void clearFaults() {
        rules.clear();
    }

    void addFault(final FaultRule rule) {
        rules.add(rule);
    }

    void removeFault(final FaultRule rule) {
        rules.remove(rule);
    }

    // ------------------------------------------------------------------
    // Proxy internals
    // ------------------------------------------------------------------

    private void acceptLoop() {
        while (running.get()) {
            try {
                final Socket client = serverSocket.accept();
                final Socket broker = new Socket(targetHost, targetPort);
                final Connection conn = new Connection();
                threadPool.submit(() -> pumpRequests(client, broker, conn));
                threadPool.submit(() -> pumpResponses(broker, client, conn));
            } catch (final Exception e) {
                if (running.get()) {
                    LOG.warn("accept loop error", e);
                }
            }
        }
    }

    /** Per-connection state: correlationId -> request header, so responses can be decoded/matched. */
    private static final class Connection {
        private final Map<Integer, RequestHeader> inflight = new ConcurrentHashMap<>();
    }

    // client -> broker: forward verbatim, recording each request header for response decoding.
    private void pumpRequests(final Socket client, final Socket broker, final Connection conn) {
        try (client; broker;
             DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(broker.getOutputStream())) {
            byte[] frame;
            while (running.get() && (frame = readFrame(in)) != null) {
                try {
                    final RequestHeader header = RequestHeader.parse(ByteBuffer.wrap(frame));
                    conn.inflight.put(header.correlationId(), header);
                } catch (final Exception parseErr) {
                    LOG.debug("could not parse request header (forwarding anyway)", parseErr);
                }
                writeFrame(out, frame);
            }
        } catch (final Exception e) {
            LOG.debug("request pump closed", e);
        }
    }

    // broker -> client: rewrite for routing and/or apply a matching fault rule; otherwise forward verbatim.
    private void pumpResponses(final Socket broker, final Socket client, final Connection conn) {
        try (broker; client;
             DataInputStream in = new DataInputStream(broker.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
            byte[] frame;
            while (running.get() && (frame = readFrame(in)) != null) {
                final int correlationId = ByteBuffer.wrap(frame).getInt(0);
                final RequestHeader reqHeader = conn.inflight.remove(correlationId);

                if (reqHeader == null) {
                    writeFrame(out, frame);
                    continue;
                }

                final ApiKeys apiKey = reqHeader.apiKey();
                final boolean routing = apiKey == ApiKeys.METADATA || apiKey == ApiKeys.FIND_COORDINATOR;
                final FaultRule fired = firstFiringRule(apiKey);

                if (fired != null && fired.action() == FaultRule.Action.DISCONNECT) {
                    LOG.info("Fault: dropping connection on {} response ({})", apiKey, fired);
                    break; // closes both sockets via try-with-resources
                }

                if (!routing && fired == null) {
                    writeFrame(out, frame);
                    continue;
                }

                writeFrame(out, transform(reqHeader, frame, routing, fired));
            }
        } catch (final Exception e) {
            LOG.debug("response pump closed", e);
        }
    }

    // Decode -> (routing rewrite and/or error inject) -> re-encode. Falls back to the original bytes on error.
    private byte[] transform(final RequestHeader reqHeader, final byte[] frame,
                             final boolean routing, final FaultRule fired) {
        final ApiKeys apiKey = reqHeader.apiKey();
        final short version = reqHeader.apiVersion();
        try {
            final AbstractResponse response = AbstractResponse.parseResponse(ByteBuffer.wrap(frame), reqHeader);

            if (routing) {
                applyRouting(response);
            }
            if (fired != null && fired.action() == FaultRule.Action.INJECT_ERROR) {
                ERROR_SETTERS.get(apiKey).accept(response, fired.error());
                LOG.info("Fault: injected {} into {} response ({})", fired.error(), apiKey, fired);
            }

            final ResponseHeaderData headerData = new ResponseHeaderData().setCorrelationId(reqHeader.correlationId());
            final ByteBuffer bb = RequestUtils.serialize(
                    headerData, apiKey.responseHeaderVersion(version), response.data(), version);
            final byte[] out = new byte[bb.remaining()];
            bb.get(out);
            return out;
        } catch (final Exception e) {
            LOG.warn("failed to transform {} v{} response; forwarding verbatim", apiKey, version, e);
            return frame;
        }
    }

    private void applyRouting(final AbstractResponse response) {
        if (response instanceof MetadataResponse) {
            ((MetadataResponse) response).data().brokers().forEach(b -> b.setHost(proxyHost).setPort(proxyPort));
        } else if (response instanceof FindCoordinatorResponse) {
            final FindCoordinatorResponse fc = (FindCoordinatorResponse) response;
            fc.data().setHost(proxyHost).setPort(proxyPort);
            fc.data().coordinators().forEach(c -> c.setHost(proxyHost).setPort(proxyPort));
        }
    }

    private FaultRule firstFiringRule(final ApiKeys apiKey) {
        FaultRule chosen = null;
        for (final FaultRule rule : rules) {
            if (rule.apiKey() == apiKey && rule.shouldFire() && chosen == null) {
                chosen = rule; // keep evaluating so every same-API rule still counts its match
            }
        }
        return chosen;
    }

    /** Reads one length-prefixed Kafka frame (without the 4-byte length). Returns null on clean EOF/close. */
    private static byte[] readFrame(final DataInputStream in) {
        try {
            final int size = in.readInt();
            final byte[] frame = new byte[size];
            in.readFully(frame);
            return frame;
        } catch (final EOFException eof) {
            return null;
        } catch (final Exception e) {
            return null;
        }
    }

    private static void writeFrame(final DataOutputStream out, final byte[] frame) throws Exception {
        out.writeInt(frame.length);
        out.write(frame);
        out.flush();
    }

    @Override
    public void close() {
        running.set(false);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (final Exception ignored) {
            // closing
        }
        threadPool.shutdownNow();
    }
}
