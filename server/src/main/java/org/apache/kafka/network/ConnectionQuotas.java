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
package org.apache.kafka.network;

import kafka.network.SocketServer;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.ListenerReconfigurable;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.quota.QuotaUtils;

import com.yammer.metrics.core.Meter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConnectionQuotas extends AbstractKafkaConfig implements Logging, AutoCloseable {
  private AbstractKafkaConfig config;
  private Time time;
  private Metrics metrics;
  private volatile int defaultMaxConnectionsPerIp;
  private volatile HashMap<InetAddress, Integer> maxConnectionsPerIpOverrides = new HashMap<>();
  private volatile int brokerMaxConnections;
  private final ListenerName interBrokerListenerName;
  private final HashMap<InetAddress, Integer> counts = new HashMap<>();

  // Listener counts and configs are synchronized on `counts`
  private final HashMap<ListenerName, Integer> listenerCounts = new HashMap<>();
  private final HashMap<ListenerName, ListenerConnectionQuota> maxConnectionsPerListener = new HashMap<>();
  private volatile int totalCount = 0;
  // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on `counts`
  private volatile int defaultConnectionRatePerIp = QuotaConfig.IP_CONNECTION_RATE_DEFAULT.intValue();
  private final ConcurrentHashMap<InetAddress, Integer> connectionRatePerIp = new ConcurrentHashMap<>();
  // sensor that tracks broker-wide connection creation rate and limit (quota)
  private final Sensor brokerConnectionRateSensor;
  private final long maxThrottleTimeMs;
  private static final String MetricsGroup = "socket-server-metrics";
  private static final String ListenerMetricTag = "listener";

  public ConnectionQuotas(AbstractKafkaConfig config, Time time, Metrics metrics) {
    this.config = config;
    this.time = time;
    this.metrics = metrics;
    this.defaultMaxConnectionsPerIp = config.maxConnectionsPerIp;
    this.maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> {return InetAddress.getByName(entry.getKey());}, Map.Entry::getValue));
    this.brokerMaxConnections = config.maxConnections();
    this.interBrokerListenerName = config.interBrokerListenerName();
    this.brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(
        config.maxConnectionCreationRate(),
        ConnectionQuotaEntity.brokerQuotaEntity());
    this.maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaConfig().quotaWindowSizeSeconds());
  }

  public void inc(ListenerName listenerName, InetAddress address, Meter acceptorBlockedPercentMeter) {
    synchronized (counts) {
      waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter);
      recordIpConnectionMaybeThrottle(listenerName, address);
      int count = counts.getOrDefault(address, 0);
      counts.put(address, count + 1);
      totalCount += 1;
      if (listenerCounts.containsKey(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts.get(listenerName) + 1);
      }
      int max = this.maxConnectionsPerIpOverrides.getOrDefault(address, this.defaultMaxConnectionsPerIp);
      if (count >= max)
        throw new TooManyConnectionsException(address, max);
    }
  }

  void updateMaxConnectionsPerIp(int maxConnectionsPerIp) {
    this.defaultMaxConnectionsPerIp = maxConnectionsPerIp;
  }

  void updateMaxConnectionsPerIpOverride(Map<String, Integer> overrideQuotas) {
    overrideQuotas
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> {
              try {return InetAddress.getByName(entry.getKey());}
              catch (UnknownHostException e) {throw new RuntimeException(e);}}, Map.Entry::getValue));
  }

  void updateBrokerMaxConnections(int maxConnections) {
    synchronized (counts) {
      brokerMaxConnections = maxConnections;
      counts.notifyAll();
    }
  }

  void updateBrokerMaxConnectionRate(int maxConnectionRate) {
    // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
    // the rate limit increases, because it is just one connection per listener and the code is simpler that way
    updateConnectionRateQuota(maxConnectionRate, ConnectionQuotaEntity.brokerQuotaEntity());
  }

  /**
   * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
   * If an IP is given, metric config will be updated only for the given IP, otherwise
   * all metric configs will be checked and updated if required.
   *
   * @param ip ip to update or default if Optional.empty()
   * @param maxConnectionRate new connection rate, or resets entity to default if Optional.empty()
   */
  public void updateIpConnectionRateQuota(Optional<InetAddress> ip, Optional<Integer> maxConnectionRate) {
    synchronized (this) {
      if (ip.isPresent()) {
        InetAddress address = ip.get();
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        synchronized (counts) {
          if (maxConnectionRate.isPresent()) {
            int rate = maxConnectionRate.get();
            info("Updating max connection rate override for " + address + " to " + rate);
            connectionRatePerIp.put(address, rate);
          } else {
            info("Removing max connection rate override for " + address);
            connectionRatePerIp.remove(address);
          }
        }
        updateConnectionRateQuota(connectionRateForIp(address), ConnectionQuotaEntity.ipQuotaEntity(address));
      } else {
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        synchronized (counts) {
          this.defaultConnectionRatePerIp = maxConnectionRate.orElse(QuotaConfig.IP_CONNECTION_RATE_DEFAULT.intValue());
        }
        info("Updated default max IP connection rate to " + defaultConnectionRatePerIp);
        metrics.metrics().forEach((metricName, metric) -> {
                  if (isIpConnectionRateMetric(metricName)) {
                    int quota = connectionRateForIp(InetAddress.getByName(metricName.tags().get(ConnectionQuotaEntity.IP_METRIC_TAG)));
                    if (shouldUpdateQuota(metric, quota)) {
                      debug("Updating existing connection rate quota config for " + metricName.tags() + " to " + quota);
                      metric.config(rateQuotaMetricConfig(quota));
                    }
                  }
        }
        );
      }
    }
  }

  private boolean isIpConnectionRateMetric(MetricName metricName) {
    return metricName.name().equals(ConnectionQuotaEntity.CONNECTION_RATE_METRIC_NAME) &&
           metricName.group().equals(MetricsGroup) &&
           metricName.tags().containsKey(ConnectionQuotaEntity.IP_METRIC_TAG);
  }

  private boolean shouldUpdateQuota(KafkaMetric metric, int quotaLimit) {
    return quotaLimit != metric.config().quota().bound();
  }

  // Visible for testing
  public int connectionRateForIp(InetAddress ip) {
    return connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp);
  }

  void addListener(AbstractKafkaConfig config, ListenerName listenerName) {
    synchronized (counts) {
      if (!maxConnectionsPerListener.containsKey(listenerName)) {
        ListenerConnectionQuota newListenerQuota = new ListenerConnectionQuota(counts, listenerName);
        maxConnectionsPerListener.put(listenerName, newListenerQuota);
        listenerCounts.put(listenerName, 0);
        config.addReconfigurable(newListenerQuota);
        newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix()));
      }
      counts.notifyAll();
    }
  }

  void removeListener(AbstractKafkaConfig config, ListenerName listenerName) {
    synchronized (counts) {
      ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.remove(listenerName);
      if (listenerQuota != null) {
        listenerCounts.remove(listenerName);
        // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
        // so it is safe to remove sensor here
        listenerQuota.close();
        counts.notifyAll(); // wake up any waiting acceptors to close cleanly
        config.removeReconfigurable(listenerQuota);
      }
    }
  }

  public void dec(ListenerName listenerName, InetAddress address) {
    synchronized (counts) {
      Integer count = counts.get(address);
      if (count == null)
        throw new IllegalArgumentException("Attempted to decrease connection count for address with no connections, address: " + address);
      if (count == 1)
        counts.remove(address);
      else
        counts.put(address, count - 1);

      if (totalCount <= 0)
        error("Attempted to decrease total connection count for broker with no connections");
      totalCount -= 1;

      if (maxConnectionsPerListener.containsKey(listenerName)) {
        int listenerCount = listenerCounts.get(listenerName);
        if (listenerCount == 0)
          error("Attempted to decrease connection count for listener " + listenerName + " with no connections");
        else
          listenerCounts.put(listenerName, listenerCount - 1);
      }
      counts.notifyAll(); // wake up any acceptors waiting to process a new connection since listener connection limit was reached
    }
  }

  public Integer get(InetAddress address) {
    synchronized (counts) {
      return counts.getOrDefault(address, 0);
    }
  }

  private void waitForConnectionSlot(ListenerName listenerName, Meter acceptorBlockedPercentMeter) {
    synchronized (counts) {
      long startThrottleTimeMs = time.milliseconds();
      long throttleTimeMs = Math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0);

      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        long startNs = time.nanoseconds();
        long endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs;
        long remainingThrottleTimeMs = throttleTimeMs;
        do {
            counts.wait(remainingThrottleTimeMs);
            remainingThrottleTimeMs = Math.max(endThrottleTimeMs - time.milliseconds(), 0);
        } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName));
        acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs);
      }
    }
  }

  // This is invoked in every poll iteration and we close one LRU connection in an iteration
  // if necessary
  public boolean maxConnectionsExceeded(ListenerName listenerName) {
    return totalCount > brokerMaxConnections && !protectedListener(listenerName);
  }

  private boolean connectionSlotAvailable(ListenerName listenerName) {
    if (listenerCounts.get(listenerName) >= maxListenerConnections(listenerName))
      return false;
    else if (protectedListener(listenerName))
      return true;
    else
      return totalCount < brokerMaxConnections;
  }

  private boolean protectedListener(ListenerName listenerName) {
    return interBrokerListenerName.equals(listenerName) && listenerCounts.size() > 1;
  }

  private int maxListenerConnections(ListenerName listenerName) {
    ListenerConnectionQuota quota = maxConnectionsPerListener.get(listenerName);
    return quota != null ? quota.maxConnections : Integer.MAX_VALUE;
  }

  /**
   * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
   * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
   *
   * @param listenerName listener for which calculate the delay
   * @param timeMs current time in milliseconds
   * @return delay in milliseconds
   */
  private long recordConnectionAndGetThrottleTimeMs(ListenerName listenerName, long timeMs) {
    if (protectedListener(listenerName)) {
      return recordAndGetListenerThrottleTime(0, timeMs);
    } else {
      long brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs);
      return recordAndGetListenerThrottleTime(brokerThrottleTimeMs, timeMs);
    }
  }

  private long recordAndGetListenerThrottleTime(long minThrottleTimeMs, long timeMs) {
    ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.get(listenerName);
    if (listenerQuota != null) {
      long listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs);
      long throttleTimeMs = Math.max(minThrottleTimeMs, listenerThrottleTimeMs);
      // record throttle time due to hitting connection rate quota
      if (throttleTimeMs > 0) {
        listenerQuota.listenerConnectionRateThrottleSensor.record((double) throttleTimeMs, timeMs);
      }
      return throttleTimeMs;
    }
    return 0;
  }

  /**
   * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate, we
   * also un-record the listener and broker connection if the IP gets throttled.
   *
   * @param listenerName listener to un-record connection
   * @param throttleMs IP throttle time to record for listener
   * @param timeMs current time in milliseconds
   */
  private void updateListenerMetrics(ListenerName listenerName, long throttleMs, long timeMs) {
    if (!protectedListener(listenerName)) {
      brokerConnectionRateSensor.record(-1.0, timeMs, false);
    }
    
    ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.get(listenerName);
    if (listenerQuota != null) {
      listenerQuota.ipConnectionRateThrottleSensor.record((double) throttleMs, timeMs);
      listenerQuota.connectionRateSensor.record(-1.0, timeMs, false);
    }
  }

  /**
   * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
   * If the connection would cause an IP quota violation, un-record the connection for both IP,
   * listener, and broker connection rate and throw a ConnectionThrottledException. Calls to
   * this function must be performed with the counts lock to ensure that reading the IP
   * connection rate quota and creating the sensor's metric config is atomic.
   *
   * @param listenerName listener to unrecord connection if throttled
   * @param address ip address to record connection
   */
  private void recordIpConnectionMaybeThrottle(ListenerName listenerName, InetAddress address) {
    int connectionRateQuota = connectionRateForIp(address);
    boolean quotaEnabled = connectionRateQuota != QuotaConfig.IP_CONNECTION_RATE_DEFAULT;
    if (quotaEnabled) {
      Sensor sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, ConnectionQuotaEntity.ipQuotaEntity(address));
      long timeMs = time.milliseconds();
      long throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs);
      if (throttleMs > 0) {
        trace("Throttling " + address + " for " + throttleMs + " ms");
        // unrecord the connection since we won't accept the connection
        sensor.record(-1.0, timeMs, false);
        updateListenerMetrics(listenerName, throttleMs, timeMs);
        throw new ConnectionThrottledException(address, timeMs, throttleMs);
      }
    }
  }

  /**
   * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
   * in milliseconds if quota got violated
   * @param sensor sensor to record connection
   * @param timeMs current time in milliseconds
   * @return throttle time in milliseconds if quota got violated, otherwise 0
   */
  private long recordAndGetThrottleTimeMs(Sensor sensor, long timeMs) {
    try {
      sensor.record(1.0, timeMs);
      return 0;
    } catch (QuotaViolationException e) {
      long throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).intValue();
      debug("Quota violated for sensor (" + sensor.name() + "). Delay time: " + throttleTimeMs + " ms");
      return throttleTimeMs;
    }
  }

  /**
   * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
   * listener or broker-wide, if listener is not provided.
   * @param quotaLimit connection creation rate quota
   * @param connectionQuotaEntity entity to create the sensor for
   */
  private Sensor getOrCreateConnectionRateQuotaSensor(int quotaLimit, ConnectionQuotaEntity connectionQuotaEntity) {
    Sensor sensor = metrics.getSensor(connectionQuotaEntity.sensorName());
    if (sensor == null) {
      sensor = metrics.sensor(
          connectionQuotaEntity.sensorName(),
          rateQuotaMetricConfig(quotaLimit),
          connectionQuotaEntity.sensorExpiration()
      );
      sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate(), null);
    }
    return sensor;
  }

  /**
   * Updates quota configuration for a given connection quota entity
   */
  private void updateConnectionRateQuota(int quotaLimit, ConnectionQuotaEntity connectionQuotaEntity) {
    KafkaMetric metric = metrics.metric(connectionRateMetricName(connectionQuotaEntity));
    if (metric != null) {
      metric.config(rateQuotaMetricConfig(quotaLimit));
      info("Updated " + connectionQuotaEntity.metricName() + " max connection creation rate to " + quotaLimit);
    }
  }

  private MetricName connectionRateMetricName(ConnectionQuotaEntity connectionQuotaEntity) {
    return metrics.metricName(
        connectionQuotaEntity.metricName(),
        MetricsGroup,
        "Tracking rate of accepting new connections (per second)",
        connectionQuotaEntity.metricTags());
  }

  private MetricConfig rateQuotaMetricConfig(int quotaLimit) {
    return new MetricConfig()
            .timeWindow((long) (this.config.quotaConfig().quotaWindowSizeSeconds()), TimeUnit.SECONDS)
            .samples(this.config.quotaConfig().numQuotaSamples())
            .quota(new Quota(quotaLimit, true));
  }

//  @Override
  public void close() {
    metrics.removeSensor(brokerConnectionRateSensor.name());
    for (ListenerConnectionQuota quota : maxConnectionsPerListener.values()) {
      quota.close();
    }
  }
  /**
   * * Close `channel` and decrement the connection count.
   * */
  public void closeChannel(Logging log, ListenerName listenerName, SocketChannel channel) {
    if (channel != null) {
      log.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
      dec(listenerName, channel.socket().getInetAddress());
      SocketServer.closeSocket(channel);
    }
  }
  public class ListenerConnectionQuota extends AutoCloseable implements ListenerReconfigurable {
    private final Object lock;
    private final ListenerName listener;
    private volatile int _maxConnections = Integer.MAX_VALUE;
    protected final Sensor connectionRateSensor;
    protected final Sensor listenerConnectionRateThrottleSensor;
    protected final Sensor ipConnectionRateThrottleSensor;

    public ListenerConnectionQuota(Object lock, ListenerName listener) {
      this.lock = lock;
      this.listener = listener;
      this.connectionRateSensor = getOrCreateConnectionRateQuotaSensor(Integer.MAX_VALUE, ConnectionQuotaEntity.listenerQuotaEntity(listener.value()));
      this.listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.LISTENER_THROTTLE_PREFIX);
      this.ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.IP_THROTTLE_PREFIX);
    }

    public int maxConnections() {
      return _maxConnections;
    }

    @Override
    public ListenerName listenerName() {
      return listener;
    }

    @Override
    public void configure(Map<String, ?> configs) {
      _maxConnections = maxConnections(configs);
      updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value()));
    }

    @Override
    public Set<String> reconfigurableConfigs() {
      return Set(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
      int value = maxConnections(configs);
      if (value <= 0)
        throw new ConfigException("Invalid " + SocketServerConfigs.MAX_CONNECTIONS_CONFIG + " " + value);
      int rate = maxConnectionCreationRate(configs);
      if (rate <= 0)
        throw new ConfigException("Invalid " + SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG + " " + rate);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
      synchronized (lock) {
        _maxConnections = maxConnections(configs);
        updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value()));
        lock.notifyAll();
      }
    }

    public void close() {
      metrics.removeSensor(connectionRateSensor.name());
      metrics.removeSensor(listenerConnectionRateThrottleSensor.name());
      metrics.removeSensor(ipConnectionRateThrottleSensor.name());
    }

    private int maxConnections(Map<String, ?> configs) {
      Object value = configs.get(SocketServerConfigs.MAX_CONNECTIONS_CONFIG);
      return value != null ? Integer.parseInt(value.toString()) : Integer.MAX_VALUE;
    }

    private int maxConnectionCreationRate(Map<String, ?> configs) {
      Object value = configs.get(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);
      return value != null ? Integer.parseInt(value.toString()) : Integer.MAX_VALUE;
    }

    /**
     * Creates sensor for tracking the average throttle time on this listener due to hitting broker/listener connection
     * rate or IP connection rate quota. The average is out of all throttle times > 0, which is consistent with the
     * bandwidth and request quota throttle time metrics.
     */
    private Sensor createConnectionRateThrottleSensor(String throttlePrefix) {
      Sensor sensor = metrics.sensor(throttlePrefix + "ConnectionRateThrottleTime-" + listener.value());
      Map<String, String> tags = new HashMap<>();
      tags.put(ListenerMetricTag, listener.value());
      MetricName metricName = metrics.metricName(
          throttlePrefix + "connection-accept-throttle-time",
          MetricsGroup,
          "Tracking average throttle-time, out of non-zero throttle times, per listener",
          tags);
      sensor.add(metricName, new Avg());
      return sensor;
    }
  }
}