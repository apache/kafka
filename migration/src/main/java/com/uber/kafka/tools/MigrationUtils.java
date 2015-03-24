// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;

import scala.collection.Iterator;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Utility methods for Kafka 0.7 to Kafak 0.8 migrator.
 */
public class MigrationUtils {

    private static final Logger LOGGER = Logger.getLogger(MigrationUtils.class);

    private static final int ZK_CONN_TIMEOUT_MS = 5 * 1000;
    private static final int ZK_SOCKET_TIMEOUT_MS = 30 * 1000;

    private static final Joiner OR_DELIMITER = Joiner.on('|');

    private static final MigrationUtils INSTANCE = new MigrationUtils();

    // For tests.
    MigrationUtils() {}

    public static MigrationUtils get() {
        return INSTANCE;
    }

    public String rewriteTopicWhitelist(String kafka08ZKHosts, String whitelist) {
        return getTopicList(kafka08ZKHosts, whitelist, true);
    }

    public String rewriteTopicBlacklist(String kafka08ZKHosts, String blacklist) {
        return getTopicList(kafka08ZKHosts, blacklist, false);
    }

    public ZkClient newZkClient(String zkServers) {
        return new ZkClient(zkServers, ZK_CONN_TIMEOUT_MS, ZK_SOCKET_TIMEOUT_MS,
            new BytesPushThroughSerializer());
    }

    private String getTopicList(String kafka08ZKHosts, String topicList, boolean isWhitelist) {
        Pattern pattern = Pattern.compile(topicList);
        List<String> allTopics = getAllTopicsInKafka08(kafka08ZKHosts);
        List<String> filteredTopics = Lists.newArrayList();
        for (String topic : allTopics) {
            Matcher matcher = pattern.matcher(topic);
            if (matcher.find() ^ !isWhitelist) {
                filteredTopics.add(topic);
            } else {
                LOGGER.warn("Attempting to migrate topic that doesn't exist in " +
                    "kafka8, topic: " + topic);
            }
        }
        return OR_DELIMITER.join(filteredTopics);
    }

    public List<String> getAllTopicsInKafka08(String kafka08ZKHosts) {
        ZkClient zkClient = newZkClient(kafka08ZKHosts);
        try {
            Iterator<String> allTopics = ZkUtils.getAllTopics(zkClient).toIterator();
            List<String> res = Lists.newArrayList();
            while (allTopics.hasNext()) {
                res.add(allTopics.next());
            }
            return res;
        } finally {
            zkClient.close();
        }
    }

}
