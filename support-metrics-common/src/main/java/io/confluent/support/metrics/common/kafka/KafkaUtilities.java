/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.common.kafka;

import kafka.cluster.EndPoint;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import kafka.admin.AdminOperationException;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.cluster.Broker;
import kafka.log.LogConfig;
import kafka.server.BrokerShuttingDown;
import kafka.server.KafkaServer;
import kafka.server.PendingControlledShutdown;
import kafka.server.RunningAsBroker;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaUtilities {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtilities.class);

  /**
   * When we verify if a topic is created, this enum keeps track of whether the desired #replicas
   * and #partitions match exactly what we asked for, or are less than what we asked for, or have an
   * unacceptable value (i.e., 0).
   */
  public enum VerifyTopicState {

    Exactly(0), Less(1), Greater(2), Inadequate(3);

    private final int stateId;

    VerifyTopicState(int stateId) {
      this.stateId = stateId;
    }

    public int getStateId() {
      return stateId;
    }

  }

  /**
   * Get the total number of topics in the cluster by querying ZooKeeper.
   *
   * @return The total number of topics in the cluster, or -1 if there was an error.
   * @throws IllegalArgumentException if zkClient is null
   */
  public long getNumTopics(KafkaZkClient zkClient) {
    Objects.requireNonNull(zkClient, "zkClient must not be null");

    try {
      Seq<String> topics = zkClient.getAllTopicsInCluster();
      return topics.length();
    } catch (Exception e) {
      log.error("Could not retrieve number of topics from ZooKeeper: {}", e.getMessage());
      return -1L;
    }
  }

  /**
   * Gets a list of servers that are up in the cluster
   *
   * @param maxNumServers Maximum number of bootstrap servers that should be returned.  Note that
   *     less servers may be returned than the maximum.
   * @return A list of bootstrap servers, or an empty list if there are none or if there were
   *     errors.  Note that only servers with PLAINTEXT ports will be returned.
   */
  public List<String> getBootstrapServers(KafkaZkClient zkClient, int maxNumServers) {
    Objects.requireNonNull(zkClient, "zkClient must not be null");

    if (maxNumServers < 1) {
      throw new IllegalArgumentException("maximum number of requested servers must be >= 1");
    }

    // Note that we only support PLAINTEXT ports for this version
    List<Broker> brokers = JavaConversions.seqAsJavaList(zkClient.getAllBrokersInCluster());
    if (brokers == null) {
      return Collections.emptyList();
    } else {
      List<String> bootstrapServers = new ArrayList<>();
      for (Broker broker : brokers) {
        for (EndPoint endPoint : JavaConversions.seqAsJavaList(broker.endPoints())) {
          if (endPoint.listenerName().value().equals("PLAINTEXT")) {
            bootstrapServers.add(endPoint.connectionString());
            if (bootstrapServers.size() == maxNumServers) {
              break;
            }
          }
        }
      }

      return bootstrapServers;
    }
  }

  /**
   * Creates a topic in Kafka, if it is not already there, and verifies that it is properly
   * created. After the method returns (true), it guarantees that leaders are elected for every
   * new topic partition, but does not guarantee that all metadata is propagated to all the brokers.
   *
   * @param partitions  Desired number of partitions
   * @param replication Desired number of replicas
   * @param retentionMs Desired retention time in milliseconds
   * @return True if topic was created and verified successfully. False if topic could not be
   *     created, or it is created but verification reveals that the number of replicas or
   *     partitions have dropped to unacceptable levels.
   */
  public boolean createAndVerifyTopic(
      KafkaZkClient zkClient,
      String topic,
      int partitions,
      int replication,
      long retentionMs
  ) {
    Objects.requireNonNull(zkClient, "zkClient must not be null");

    validateTopicParams(topic, partitions, replication, retentionMs);

    boolean topicCreated = true;
    try {
      if (zkClient.topicExists(topic)) {
        return verifySupportTopic(zkClient, topic, partitions, replication) != VerifyTopicState.Inadequate;
      }
      Seq<Broker> brokerList = zkClient.getAllBrokersInCluster();
      int actualReplication = Math.min(replication, brokerList.size());
      if (actualReplication < replication) {
        log.warn(
            "The replication factor of topic {} will be set to {}, which is less than the "
            + "desired replication factor of {} (reason: this cluster contains only {} brokers).  "
            + "If you happen to add more brokers to this cluster, then it is important to increase "
            + "the replication factor of the topic to eventually {} to ensure reliable and "
            + "durable metrics collection.",
            topic,
            actualReplication,
            replication,
            brokerList.size(),
            replication
        );
      }

      Properties metricsTopicProps = new Properties();
      metricsTopicProps.put(LogConfig.RetentionMsProp(), String.valueOf(retentionMs));
      log.info("Attempting to create topic {} with {} replicas, assuming {} total brokers",
               topic, actualReplication, brokerList.size()
      );
      AdminZkClient adminClient = new AdminZkClient(zkClient);
      adminClient.createTopic(
          topic,
          partitions,
          actualReplication,
          metricsTopicProps,
          Disabled$.MODULE$
      );
      // wait until leader is elected for every topic partition we created
      for (int part = 0; part < partitions; ++part) {
        waitUntilLeaderIsElected(zkClient, topic, part, 30000L);
      }
    } catch (TopicExistsException te) {
      log.info("Topic {} already exists", topic);
      topicCreated = false;
    } catch (AdminOperationException e) {
      topicCreated = false;
      log.error("Could not create topic {}: {}", topic, e.getMessage());
    } catch (TimeoutException toe) {
      topicCreated = false;
      log.error("Timed out waiting for leader to be elected after creating topic: {}",
                toe.getMessage());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      topicCreated = false;
      log.error("Interrupted the wait for leader to be elected after creating topic={}", topic);
    } catch (Exception e) {
      // there are several other Zookeeper exceptions possible deep in Zookeeper
      topicCreated = false;
      log.error("Zookeeper is unavailable. Could not create topic {}: {}", topic, e.getMessage());
    }

    return topicCreated;
  }

  private void validateTopicParams(
      String topic,
      int partitions,
      int replication,
      long retentionMs
  ) {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("topic must not be null or empty");
    }
    if (partitions < 1) {
      throw new IllegalArgumentException("partitions must be >= 1");
    }
    if (replication < 1) {
      throw new IllegalArgumentException("replication factor must be >= 1");
    }
    if (retentionMs <= 0) {
      throw new IllegalArgumentException("retention.ms must be >= 1");
    }
  }

  /**
   * Verifies that the Kafka topic exists and is healthy.
   *
   * @param topic Topic to be validated.
   * @param expPartitions Expected number of partitions
   * @param expReplication Expected number of replicas
   * @return an enum describing the topic state
   */
  @SuppressWarnings("unchecked")
  public VerifyTopicState verifySupportTopic(
      KafkaZkClient zkClient,
      String topic,
      int expPartitions,
      int expReplication
  ) {
    Objects.requireNonNull(zkClient, "zkClient must not be null");

    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("topic must not be null or empty");
    }
    if (expPartitions < 1) {
      throw new IllegalArgumentException("expected partitions must be >= 1");
    }
    if (expReplication < 1) {
      throw new IllegalArgumentException("expected replication factor must be >= 1");
    }

    VerifyTopicState verifyTopicState = VerifyTopicState.Exactly;
    try {
      Set<String> topics = new HashSet<>();
      topics.add(topic);
      scala.Option<scala.collection.immutable.Map<Object, Seq<Object>>> partitionAssignmentOption =
          zkClient.getPartitionAssignmentForTopics(
              JavaConversions.asScalaSet(topics).<String>toSet()).get(topic);
      if (!partitionAssignmentOption.isEmpty()) {
        scala.collection.Map partitionAssignment = partitionAssignmentOption.get();
        int actualNumPartitions = partitionAssignment.size();
        if (actualNumPartitions != expPartitions) {
          log.warn(
              "The topic {} should have only {} partitions.  Having more partitions should "
              + "not hurt but it is only needed under special circumstances.",
              topic,
              expPartitions
          );
          verifyTopicState = VerifyTopicState.Less;
        }
        int firstPartitionId = 0;
        scala.Option<Seq<Object>> replicasOfFirstPartitionOption =
            partitionAssignment.get(firstPartitionId);
        if (!replicasOfFirstPartitionOption.isEmpty()) {
          int actualReplication = replicasOfFirstPartitionOption.get().size();
          if (actualReplication < expReplication) {
            log.warn(
                "The replication factor of topic {} is {}, which is less than "
                + "the desired replication factor of {}.  If you happen to add more brokers to this"
                + " cluster, then it is important to increase the replication factor of the "
                + "topic to eventually {} to ensure reliable and durable metrics collection.",
                topic,
                actualReplication,
                expReplication,
                expReplication
            );
            verifyTopicState = VerifyTopicState.Less;
          }
        } else {
          log.error("No replicas known for partition 0 of support metrics topic {}", topic);
          verifyTopicState = VerifyTopicState.Inadequate;
        }
      } else {
        log.error("No partitions are assigned to support metrics topic {}", topic);
        verifyTopicState = VerifyTopicState.Inadequate;
      }
    } catch (Exception e) {
      // there are several Zookeeper exceptions possible deep in Zookeeper
      log.error("Zookeeper is unavailable. Could not verify topic {}", topic);
      verifyTopicState = VerifyTopicState.Inadequate;
    }

    return verifyTopicState;
  }

  public boolean isReadyForMetricsCollection(KafkaServer server) {
    return server.brokerState().currentState() == RunningAsBroker.state();
  }

  public boolean isShuttingDown(KafkaServer server) {
    return server.brokerState().currentState() == PendingControlledShutdown.state()
           || server.brokerState().currentState() == BrokerShuttingDown.state();
  }


  /**
   * Wait until the leader of a partition is elected.
   * @throws TimeoutException if the leader is unknown after timeout ms is passed.
   * @throws InterruptedException if waiting for leader election was interrupted.
   */
  private void waitUntilLeaderIsElected(
      KafkaZkClient zkClient, String topic, int partition, long timeout)
      throws InterruptedException, TimeoutException {
    if (zkClient == null) {
      throw new IllegalArgumentException("zkClient must not be null");
    }
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("topic must not be null or empty");
    }

    long startMs = Time.SYSTEM.milliseconds();
    long now;
    while ((now = Time.SYSTEM.milliseconds()) < startMs + timeout) {
      scala.Option<Object> leaderOpt =
          zkClient.getLeaderForPartition(new TopicPartition(topic, partition));
      if (leaderOpt.isDefined() && ((int) leaderOpt.get() >= 0)) {
        return;
      }
      Thread.sleep(Math.min(startMs + timeout - now, 100L));
    }
    throw new TimeoutException(
        "Timing out after " + timeout
        + "ms since a leader was not elected for topic=" + topic + " partition=" + partition);
  }

}
