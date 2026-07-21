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

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.SyncGroupResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
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
 *         proxy.disconnectOn(ApiKeys.END_TXN).once();               // the EOS "commit gap"
 *         proxy.delayResponse(ApiKeys.FETCH, Duration.ofSeconds(5)).everyTime(); // a slow/degraded broker
 *         proxy.blackholeOn(ApiKeys.JOIN_GROUP).once();              // a silent network partition
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
 *
 * <p>Fault actions differ in how "broken" they make the connection look to the client:
 * <ul>
 *   <li>{@code injectError(...)} — the broker-visible response arrives, carrying an application error code.</li>
 *   <li>{@code disconnectOn(...)} — the TCP connection is reset; most clients treat this as an immediate,
 *       unambiguous "reconnect now" signal.</li>
 *   <li>{@code delayResponse(...)} — the response arrives, but only after a delay; exercises client-side
 *       timeout paths ({@code request.timeout.ms}, session/heartbeat timeouts) that the above two can't reach.</li>
 *   <li>{@code blackholeOn(...)} — the request is dropped before it ever reaches the broker: no response, no
 *       reset. The client just waits until its own timeout fires, as with a real silent network partition.</li>
 * </ul>
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
        // Common consumer-path errors.
        ERROR_SETTERS.put(ApiKeys.FETCH, (r, e) -> {
            final FetchResponseData data = ((FetchResponse) r).data();
            data.responses().forEach(topic ->
                topic.partitions().forEach(p -> p.setErrorCode(e.code())));
        });
        ERROR_SETTERS.put(ApiKeys.OFFSET_COMMIT, (r, e) -> {
            final OffsetCommitResponseData data = ((OffsetCommitResponse) r).data();
            data.topics().forEach(topic ->
                topic.partitions().forEach(p -> p.setErrorCode(e.code())));
        });
        ERROR_SETTERS.put(ApiKeys.LIST_OFFSETS, (r, e) -> {
            final ListOffsetsResponseData data = ((ListOffsetsResponse) r).data();
            data.topics().forEach(topic ->
                topic.partitions().forEach(p -> p.setErrorCode(e.code())));
        });
        ERROR_SETTERS.put(ApiKeys.JOIN_GROUP, (r, e) -> ((JoinGroupResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.SYNC_GROUP, (r, e) -> ((SyncGroupResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.HEARTBEAT, (r, e) -> ((HeartbeatResponse) r).data().setErrorCode(e.code()));
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
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.INJECT_ERROR, error, null);
    }

    /** Drop the connection when a response of {@code apiKey} would be returned (models the EOS commit gap). */
    public FaultRule.Builder disconnectOn(final ApiKeys apiKey) {
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.DISCONNECT, null, null);
    }

    /**
     * Delay responses of {@code apiKey} by {@code delay} before forwarding them (models a slow broker or a
     * degraded network path). The delay runs on this connection's response-pump thread, so it also holds up
     * forwarding of any later responses on the same connection ("head of line" — realistic for a single slow
     * link, but worth knowing if the test also expects other in-flight requests on the same connection to
     * complete promptly).
     */
    public FaultRule.Builder delayResponse(final ApiKeys apiKey, final Duration delay) {
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.DELAY, null, delay);
    }

    /**
     * Silently drop requests of {@code apiKey} before they ever reach the broker: no response, no connection
     * reset — the client just sits waiting until its own timeout (e.g. {@code request.timeout.ms}) fires.
     * Models a network black hole (a firewall or partition silently dropping packets), as distinct from
     * {@link #disconnectOn(ApiKeys)}'s immediate, unambiguous connection reset.
     */
    public FaultRule.Builder blackholeOn(final ApiKeys apiKey) {
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.BLACKHOLE, null, null);
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
    // Unless a BLACKHOLE rule fires for the request's API, in which case it is silently dropped here and
    // never reaches the broker at all (the client just times out waiting for a response).
    private void pumpRequests(final Socket client, final Socket broker, final Connection conn) {
        try (client; broker;
             DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(broker.getOutputStream())) {
            byte[] frame;
            while (running.get() && (frame = readFrame(in)) != null) {
                RequestHeader header = null;
                try {
                    header = RequestHeader.parse(ByteBuffer.wrap(frame));
                } catch (final Exception parseErr) {
                    LOG.debug("could not parse request header (forwarding anyway)", parseErr);
                }

                if (header != null) {
                    final FaultRule fired = firstFiringRequestRule(header.apiKey());
                    if (fired != null) {
                        LOG.info("Fault: blackholing {} request, never forwarded to the broker ({})", header.apiKey(), fired);
                        continue;
                    }
                    conn.inflight.put(header.correlationId(), header);
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
                final FaultRule fired = firstFiringResponseRule(apiKey);

                if (fired != null && fired.action() == FaultRule.Action.DISCONNECT) {
                    LOG.info("Fault: dropping connection on {} response ({})", apiKey, fired);
                    break; // closes both sockets via try-with-resources
                }

                if (fired != null && fired.action() == FaultRule.Action.DELAY) {
                    LOG.info("Fault: delaying {} response by {} ({})", apiKey, fired.delay(), fired);
                    try {
                        Thread.sleep(fired.delay().toMillis());
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return; // proxy is shutting down
                    }
                }

                if (!routing && (fired == null || fired.action() == FaultRule.Action.DELAY)) {
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
                applyRouting(response, version);
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

    // FindCoordinatorResponse's top-level Host/Port fields are only valid for versions 0-3; version 4+ (KIP-699)
    // batches results into the Coordinators list instead, and the generated code rejects a non-default
    // top-level Host/Port at those versions. Rewrite whichever half of the schema the negotiated version
    // actually carries.
    private static final short FIND_COORDINATOR_BATCHED_VERSION = 4;

    private void applyRouting(final AbstractResponse response, final short version) {
        if (response instanceof MetadataResponse) {
            ((MetadataResponse) response).data().brokers().forEach(b -> b.setHost(proxyHost).setPort(proxyPort));
        } else if (response instanceof FindCoordinatorResponse) {
            final FindCoordinatorResponse fc = (FindCoordinatorResponse) response;
            if (version >= FIND_COORDINATOR_BATCHED_VERSION) {
                fc.data().coordinators().forEach(c -> c.setHost(proxyHost).setPort(proxyPort));
            } else {
                fc.data().setHost(proxyHost).setPort(proxyPort);
            }
        }
    }

    // Evaluated from pumpRequests: only BLACKHOLE rules match here, since that's the one action that must act
    // before the request ever reaches the broker.
    private FaultRule firstFiringRequestRule(final ApiKeys apiKey) {
        FaultRule chosen = null;
        for (final FaultRule rule : rules) {
            if (rule.apiKey() == apiKey && rule.action() == FaultRule.Action.BLACKHOLE && rule.shouldFire() && chosen == null) {
                chosen = rule; // keep evaluating so every same-API rule still counts its match
            }
        }
        return chosen;
    }

    // Evaluated from pumpResponses: every other action (a response necessarily exists to act on).
    private FaultRule firstFiringResponseRule(final ApiKeys apiKey) {
        FaultRule chosen = null;
        for (final FaultRule rule : rules) {
            if (rule.apiKey() == apiKey && rule.action() != FaultRule.Action.BLACKHOLE && rule.shouldFire() && chosen == null) {
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
