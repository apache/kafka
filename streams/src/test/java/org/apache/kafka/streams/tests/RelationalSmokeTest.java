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
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;

/**
 * This test builds on a basic relational data caricature:
 * a set of articles, each with a set of comments on them.
 * <p>
 * The particular relation in this data is that each Comment
 * has an articleId foreign-key reference to an Article.
 * <p>
 * We traverse this relation in both directions to verify
 * correct operation of Kafka Streams:
 * - Embed on each Comment a prefix of the text in the Article it references
 * aka SELECT * FROM Comment JOIN Article ON Comment.ArticleID = Article.ID
 * - Embed on each Article a count of all the Comments it has
 */
public class RelationalSmokeTest extends SmokeTestUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalSmokeTest.class);

    static final String ARTICLE_SOURCE = "in-article";
    static final String COMMENT_SOURCE = "in-comment";
    static final String ARTICLE_RESULT_SINK = "out-augmented-article";
    static final String COMMENT_RESULT_SINK = "out-augmented-comment";
    private static final String[] TOPICS = {
        ARTICLE_SOURCE,
        COMMENT_SOURCE,
        ARTICLE_RESULT_SINK,
        COMMENT_RESULT_SINK
    };

    public static String[] topics() {
        return Arrays.copyOf(TOPICS, TOPICS.length);
    }

    public static class Article {
        private final int key;
        private final long timestamp;
        private final String text;

        private Article(final int key, final long timestamp, final String text) {
            this.key = key;
            this.timestamp = timestamp;
            this.text = text;
        }

        public int getKey() {
            return key;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getText() {
            return text;
        }

        @Override
        public String toString() {
            return "Article{" +
                "key=" + key +
                ", timestamp=" + Instant.ofEpochMilli(timestamp) +
                ", text='" + text + '\'' +
                '}';
        }

        public static class ArticleSerializer implements Serializer<Article> {
            @Override
            public byte[] serialize(final String topic, final Article data) {
                final byte[] serialText = stringSerde.serializer().serialize(topic, data.getText());

                final int length = Integer.BYTES + Long.BYTES + Integer.BYTES + serialText.length;

                final ByteBuffer buffer =
                    ByteBuffer.allocate(length)
                        .putInt(data.getKey())
                        .putLong(data.getTimestamp())
                        .putInt(serialText.length)
                        .put(serialText);

                return Serdes.ByteBuffer().serializer().serialize(topic, buffer);
            }
        }

        public static class ArticleDeserializer implements Deserializer<Article> {

            public static Article deserialize(final String topic, final ByteBuffer buffer) {
                final int key = buffer.getInt();
                final long timestamp = buffer.getLong();
                final int textLength = buffer.getInt();
                final byte[] serialText = new byte[textLength];
                buffer.get(serialText);
                final String text = stringSerde.deserializer().deserialize(topic, serialText);
                return new Article(key, timestamp, text);
            }

            @Override
            public Article deserialize(final String topic, final byte[] data) {
                final ByteBuffer buffer = Serdes.ByteBuffer().deserializer().deserialize(topic, data);
                return deserialize(topic, buffer);
            }
        }

        public static class ArticleSerde implements Serde<Article> {
            @Override
            public Serializer<Article> serializer() {
                return new ArticleSerializer();
            }

            @Override
            public Deserializer<Article> deserializer() {
                return new ArticleDeserializer();
            }
        }
    }

    public static class Comment {
        private final int key;
        private final long timestamp;
        private final String text;
        private final int articleId;

        private Comment(final int key, final long timestamp, final String text, final int articleId) {
            this.key = key;
            this.timestamp = timestamp;
            this.text = text;
            this.articleId = articleId;
        }

        public int getKey() {
            return key;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getText() {
            return text;
        }

        public int getArticleId() {
            return articleId;
        }

        @Override
        public String toString() {
            return "Comment{" +
                "key=" + key +
                ", timestamp=" + Instant.ofEpochMilli(timestamp) +
                ", text='" + text + '\'' +
                ", articleId=" + articleId +
                '}';
        }

        public static class CommentSerializer implements Serializer<Comment> {

            @Override
            public byte[] serialize(final String topic, final Comment data) {
                final byte[] serialText = stringSerde.serializer().serialize(topic, data.text);

                final int length = Integer.BYTES + Long.BYTES + (Integer.BYTES + serialText.length) + Integer.BYTES;

                final ByteBuffer buffer =
                    ByteBuffer.allocate(length)
                        .putInt(data.key)
                        .putLong(data.timestamp)
                        .putInt(serialText.length)
                        .put(serialText)
                        .putInt(data.articleId);

                return Serdes.ByteBuffer().serializer().serialize(topic, buffer);
            }
        }

        public static class CommentDeserializer implements Deserializer<Comment> {

            public static Comment deserialize(final String topic, final ByteBuffer buffer) {
                final int key = buffer.getInt();
                final long timestamp = buffer.getLong();
                final int textLength = buffer.getInt();
                final byte[] textBytes = new byte[textLength];
                buffer.get(textBytes);
                final String text = stringSerde.deserializer().deserialize(topic, textBytes);
                final int articleId = buffer.getInt();

                return new Comment(key, timestamp, text, articleId);
            }

            @Override
            public Comment deserialize(final String topic, final byte[] data) {
                final ByteBuffer buffer = Serdes.ByteBuffer().deserializer().deserialize(topic, data);
                return deserialize(topic, buffer);
            }
        }

        public static class CommentSerde implements Serde<Comment> {

            @Override
            public Serializer<Comment> serializer() {
                return new CommentSerializer();
            }

            @Override
            public Deserializer<Comment> deserializer() {
                return new CommentDeserializer();
            }
        }
    }

    public static final class DataSet {
        private final Article[] articles;
        private final Comment[] comments;

        private DataSet(final Article[] articles, final Comment[] comments) {
            this.articles = articles;
            this.comments = comments;
        }

        public Article[] getArticles() {
            return articles;
        }

        public Comment[] getComments() {
            return comments;
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(articles.length).append(" Articles").append('\n');
            for (final Article article : articles) {
                stringBuilder.append("  ").append(article).append('\n');
            }
            stringBuilder.append(comments.length).append(" Comments").append("\n");
            for (final Comment comment : comments) {
                stringBuilder.append("  ").append(comment).append('\n');
            }
            return stringBuilder.toString();
        }

        public static DataSet generate(final int numArticles, final int numComments) {
            // generate four days' worth of data, starting right now (to avoid broker retention/compaction)
            final int timeSpan = 1000 * 60 * 60 * 24 * 4;
            final long dataStartTime = System.currentTimeMillis();
            final long dataEndTime = dataStartTime + timeSpan;

            // Explicitly create a seed so we can we can log.
            // If we are debugging a failed run, we can deterministically produce the same dataset
            // by plugging in the seed from that run.
            final long seed = new Random().nextLong();
            final Random random = new Random(seed);
            LOG.info("Dataset PRNG seed: {}", seed);
            final Iterator<Integer> articlesToCommentOnSequence = zipfNormal(random, numArticles);

            final Article[] articles = new Article[numArticles];
            final Comment[] comments = new Comment[numComments];

            // first generate the articles (note: out of order)
            for (int i = 0; i < numArticles; i++) {
                final long timestamp = random.nextInt(timeSpan) + dataStartTime;
                final String text = randomText(random, 2000);
                articles[i] = new Article(i, timestamp, text);
            }

            // then spend the rest of the time generating the comments
            for (int i = 0; i < numComments; i++) {
                final int articleId = articlesToCommentOnSequence.next();
                final long articleTimestamp = articles[articleId].getTimestamp();
                // comments get written after articles
                final long timestamp = random.nextInt((int) (dataEndTime - articleTimestamp)) + articleTimestamp;
                final String text = randomText(random, 200);
                final Comment comment = new Comment(i, timestamp, text, articleId);
                comments[i] = comment;
            }
            return new DataSet(articles, comments);
        }

        /**
         * Rough-and-ready random text generator. Creates a text with a length normally
         * distributed about {@code avgLength} with a standard deviation of 1/3rd {@code avgLength}.
         * Each letter is drawn uniformly from a-z.
         */
        private static String randomText(final Random random, final int avgLength) {
            final int lowChar = 97; // letter 'a'
            final int highChar = 122; // letter 'z'

            final int length = Math.max(0, (int) (random.nextGaussian() * avgLength / 3.0) + avgLength);
            final char[] chars = new char[length];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = (char) (random.nextInt(highChar - lowChar) + lowChar);
            }
            return new String(chars);
        }

        /**
         * Generates a keySpace number of unique keys normally distributed,
         * with the mean at 0 and stdDev 1/3 of the way through the keySpace
         * any sample more than 3 standard deviations from the mean are assigned to the 0 key.
         * <p>
         * This is designed to roughly balance the key properties of two dominant real-world
         * data distribution: Zipfian and Normal, while also being efficient to generate.
         */
        private static Iterator<Integer> zipfNormal(final Random random, final int keySpace) {
            return new Iterator<Integer>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Integer next() {
                    final double gaussian = Math.abs(random.nextGaussian());
                    final double scaled = gaussian / 3.0; //
                    final double sample = scaled > 1.0 ? 0.0 : scaled;
                    final double keyDouble = sample * keySpace;
                    return (int) keyDouble;
                }
            };
        }

        public void produce(final String kafka, final Duration timeToSpend) throws InterruptedException {
            final Properties producerProps = new Properties();
            final String id = "RelationalSmokeTestProducer" + UUID.randomUUID();
            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, id);
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");

            final Article.ArticleSerializer articleSerializer = new Article.ArticleSerializer();
            final Comment.CommentSerializer commentSerializer = new Comment.CommentSerializer();

            final long pauseTime = timeToSpend.toMillis() / (articles.length + comments.length);

            try (final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps)) {
                int a = 0;
                int c = 0;
                while (a < articles.length || c < comments.length) {
                    final ProducerRecord<Integer, byte[]> producerRecord;
                    if (a < articles.length && c >= comments.length ||
                        a < articles.length && articles[a].getTimestamp() <= comments[c].timestamp) {
                        producerRecord = new ProducerRecord<>(
                            ARTICLE_SOURCE,
                            null,
                            articles[a].getTimestamp(),
                            articles[a].getKey(),
                            articleSerializer.serialize("", articles[a])
                        );
                        a++;
                    } else {
                        producerRecord = new ProducerRecord<>(
                            COMMENT_SOURCE,
                            null,
                            comments[c].getTimestamp(),
                            comments[c].getKey(),
                            commentSerializer.serialize("", comments[c])
                        );
                        c++;
                    }
                    producer.send(producerRecord);
                    producer.flush();
                    LOG.info("sent {} {}", producerRecord.topic(), producerRecord.key());
                    Thread.sleep(pauseTime);
                }
            }
        }
    }

    public static final class AugmentedArticle extends Article {
        private final long commentCount;

        private AugmentedArticle(final int key, final long timestamp, final String text, final long commentCount) {
            super(key, timestamp, text);
            this.commentCount = commentCount;
        }

        public long getCommentCount() {
            return commentCount;
        }

        @Override
        public String toString() {
            return "AugmentedArticle{" +
                "key=" + super.key +
                ", timestamp=" + getTimestamp() +
                ", text='" + getText() + '\'' +
                ", commentCount=" + commentCount +
                '}';
        }

        public static class AugmentedArticleSerializer implements Serializer<AugmentedArticle> {
            private final ArticleSerializer articleSerializer = new ArticleSerializer();

            @Override
            public byte[] serialize(final String topic, final AugmentedArticle data) {
                final byte[] serializedArticle = articleSerializer.serialize(topic, data);
                final int length = serializedArticle.length + Long.BYTES;
                final ByteBuffer buffer =
                    ByteBuffer.allocate(length)
                        .put(serializedArticle)
                        .putLong(data.getCommentCount());
                return Serdes.ByteBuffer().serializer().serialize(topic, buffer);
            }
        }

        public static class AugmentedArticleDeserializer implements Deserializer<AugmentedArticle> {
            @Override
            public AugmentedArticle deserialize(final String topic, final byte[] data) {
                final ByteBuffer wrap = ByteBuffer.wrap(data);

                final Article article = ArticleDeserializer.deserialize(topic, wrap);
                final long commentCount = wrap.getLong();
                return new AugmentedArticle(article.key, article.getTimestamp(), article.getText(), commentCount);
            }
        }

        public static class AugmentedArticleSerde implements Serde<AugmentedArticle> {

            @Override
            public Serializer<AugmentedArticle> serializer() {
                return new AugmentedArticleSerializer();
            }

            @Override
            public Deserializer<AugmentedArticle> deserializer() {
                return new AugmentedArticleDeserializer();
            }
        }

        public static ValueJoiner<Article, Long, AugmentedArticle> joiner() {
            return (article, commentCount) -> new AugmentedArticle(
                article.getKey(),
                article.getTimestamp(),
                article.getText(), commentCount == null ? 0 : commentCount
            );
        }
    }

    public static final class AugmentedComment extends Comment {
        private final String articlePrefix;

        private AugmentedComment(final int key,
                                 final long timestamp,
                                 final String text,
                                 final int articleId,
                                 final String articlePrefix) {
            super(key, timestamp, text, articleId);
            this.articlePrefix = articlePrefix;
        }

        public String getArticlePrefix() {
            return articlePrefix;
        }

        @Override
        public String toString() {
            return "AugmentedComment{" +
                "key=" + super.key +
                ", timestamp=" + getTimestamp() +
                ", text='" + getText() + '\'' +
                ", articleId=" + getArticleId() +
                ", articlePrefix='" + articlePrefix + '\'' +
                '}';
        }

        public static class AugmentedCommentSerializer implements Serializer<AugmentedComment> {
            private final CommentSerializer commentSerializer = new CommentSerializer();

            @Override
            public byte[] serialize(final String topic, final AugmentedComment data) {
                final byte[] serializedComment = commentSerializer.serialize(topic, data);
                final byte[] serializedPrefix = stringSerde.serializer().serialize(topic, data.getArticlePrefix());
                final int length = serializedComment.length + Integer.BYTES + serializedPrefix.length;
                final ByteBuffer buffer =
                    ByteBuffer.allocate(length)
                        .put(serializedComment)
                        .putInt(serializedPrefix.length)
                        .put(serializedPrefix);
                return Serdes.ByteBuffer().serializer().serialize(topic, buffer);
            }
        }

        public static class AugmentedCommentDeserializer implements Deserializer<AugmentedComment> {
            @Override
            public AugmentedComment deserialize(final String topic, final byte[] data) {
                final ByteBuffer wrap = ByteBuffer.wrap(data);

                final Comment comment = CommentDeserializer.deserialize(topic, wrap);
                final int prefixLength = wrap.getInt();
                final byte[] serializedPrefix = new byte[prefixLength];
                wrap.get(serializedPrefix);
                final String articlePrefix = stringSerde.deserializer().deserialize(topic, serializedPrefix);
                return new AugmentedComment(
                    comment.key,
                    comment.getTimestamp(),
                    comment.getText(),
                    comment.getArticleId(),
                    articlePrefix
                );
            }
        }

        public static class AugmentedCommentSerde implements Serde<AugmentedComment> {

            @Override
            public Serializer<AugmentedComment> serializer() {
                return new AugmentedCommentSerializer();
            }

            @Override
            public Deserializer<AugmentedComment> deserializer() {
                return new AugmentedCommentDeserializer();
            }
        }

        private static String prefix(final String text, final int length) {
            return text.length() < length ? text : text.substring(0, length);
        }

        public static ValueJoiner<Comment, Article, AugmentedComment> joiner() {
            return (comment, article) -> new AugmentedComment(
                comment.key,
                comment.getTimestamp(),
                comment.getText(),
                comment.getArticleId(),
                prefix(article.getText(), 10)
            );
        }
    }

    public static final class App {
        public static Topology getTopology() {
            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            final KTable<Integer, Article> articles =
                streamsBuilder.table(ARTICLE_SOURCE, Consumed.with(intSerde, new Article.ArticleSerde()));

            final KTable<Integer, Comment> comments =
                streamsBuilder.table(COMMENT_SOURCE, Consumed.with(intSerde, new Comment.CommentSerde()));


            final KTable<Integer, Long> commentCounts =
                comments.groupBy(
                    (key, value) -> new KeyValue<>(value.getArticleId(), (short) 1),
                    Grouped.with(Serdes.Integer(), Serdes.Short())
                )
                .count();

            articles
                .leftJoin(
                    commentCounts,
                    AugmentedArticle.joiner(),
                    Materialized.with(null, new AugmentedArticle.AugmentedArticleSerde())
                )
                .toStream()
                .to(ARTICLE_RESULT_SINK);

            comments.join(articles,
                          Comment::getArticleId,
                          AugmentedComment.joiner(),
                          Materialized.with(null, new AugmentedComment.AugmentedCommentSerde()))
                .toStream()
                .to(COMMENT_RESULT_SINK);

            return streamsBuilder.build();
        }

        public static Properties getConfig(final String broker,
                                           final String application,
                                           final String id,
                                           final String processingGuarantee,
                                           final String stateDir) {
            final Properties properties =
                mkProperties(
                    mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, application),
                        mkEntry(StreamsConfig.CLIENT_ID_CONFIG, id),
                        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee),
                        mkEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                        mkEntry(StreamsConfig.STATE_DIR_CONFIG, stateDir)
                    )
                );
            properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
            return properties;
        }

        public static KafkaStreams startSync(final String broker,
                                             final String application,
                                             final String id,
                                             final String processingGuarantee,
                                             final String stateDir) throws InterruptedException {
            final KafkaStreams kafkaStreams =
                new KafkaStreams(getTopology(), getConfig(broker, application, id, processingGuarantee, stateDir));
            final CountDownLatch startUpLatch = new CountDownLatch(1);
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
                    startUpLatch.countDown();
                }
            });
            kafkaStreams.start();
            startUpLatch.await();
            LOG.info("Streams has started.");
            return kafkaStreams;
        }

        public static boolean verifySync(final String broker, final Instant deadline) throws InterruptedException {
            final Deserializer<Integer> keyDeserializer = intSerde.deserializer();

            final Deserializer<Article> articleDeserializer = new Article.ArticleDeserializer();

            final Deserializer<AugmentedArticle> augmentedArticleDeserializer =
                new AugmentedArticle.AugmentedArticleDeserializer();

            final Deserializer<Comment> commentDeserializer = new Comment.CommentDeserializer();

            final Deserializer<AugmentedComment> augmentedCommentDeserializer =
                new AugmentedComment.AugmentedCommentDeserializer();


            final Properties consumerProperties = new Properties();
            final String id = "RelationalSmokeTestConsumer" + UUID.randomUUID();
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, id);
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (final KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
                final List<PartitionInfo> articlePartitions = consumer.partitionsFor(ARTICLE_SOURCE);
                final List<PartitionInfo> augmentedArticlePartitions = consumer.partitionsFor(ARTICLE_RESULT_SINK);
                final List<PartitionInfo> commentPartitions = consumer.partitionsFor(COMMENT_SOURCE);
                final List<PartitionInfo> augmentedCommentPartitions = consumer.partitionsFor(COMMENT_RESULT_SINK);
                final List<TopicPartition> assignment =
                    Stream.concat(
                        Stream.concat(
                            articlePartitions.stream().map(p -> new TopicPartition(p.topic(), p.partition())),
                            augmentedArticlePartitions.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                        ), 
                        Stream.concat(
                            commentPartitions.stream().map(p -> new TopicPartition(p.topic(), p.partition())),
                            augmentedCommentPartitions.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                        )
                    ).collect(toList());
                consumer.assign(assignment);
                consumer.seekToBeginning(assignment);

                final Map<Integer, Article> consumedArticles = new TreeMap<>();
                final Map<Integer, AugmentedArticle> consumedAugmentedArticles = new TreeMap<>();
                final Map<Integer, Comment> consumedComments = new TreeMap<>();
                final Map<Integer, AugmentedComment> consumedAugmentedComments = new TreeMap<>();

                boolean printedConsumedArticle = false;
                boolean printedConsumedAugmentedArticle = false;
                boolean printedConsumedComment = false;
                boolean printedConsumedAugmentedComment = false;
                boolean passed = false;

                while (!passed && Instant.now().isBefore(deadline)) {
                    boolean lastPollWasEmpty = false;
                    while (!lastPollWasEmpty) {
                        final ConsumerRecords<Integer, byte[]> poll = consumer.poll(Duration.ofSeconds(1));
                        lastPollWasEmpty = poll.isEmpty();
                        for (final ConsumerRecord<Integer, byte[]> record : poll) {
                            final Integer key = record.key();
                            switch (record.topic()) {
                                case ARTICLE_SOURCE: {
                                    final Article article = articleDeserializer.deserialize("", record.value());
                                    if (consumedArticles.containsKey(key)) {
                                        LOG.warn("Duplicate article: {} and {}", consumedArticles.get(key), article);
                                    }
                                    consumedArticles.put(key, article);
                                    break;
                                }
                                case COMMENT_SOURCE: {
                                    final Comment comment = commentDeserializer.deserialize("", record.value());
                                    if (consumedComments.containsKey(key)) {
                                        LOG.warn("Duplicate comment: {} and {}", consumedComments.get(key), comment);
                                    }
                                    consumedComments.put(key, comment);
                                    break;
                                }
                                case ARTICLE_RESULT_SINK: {
                                    final AugmentedArticle article =
                                        augmentedArticleDeserializer.deserialize("", record.value());
                                    consumedAugmentedArticles.put(key, article);
                                    break;
                                }
                                case COMMENT_RESULT_SINK: {
                                    final AugmentedComment comment =
                                        augmentedCommentDeserializer.deserialize("", record.value());
                                    consumedAugmentedComments.put(key, comment);
                                    break;
                                }
                                default:
                                    throw new IllegalArgumentException(record.toString());
                            }
                        }
                        consumer.commitSync();
                    }

                    if (!printedConsumedArticle && !consumedArticles.isEmpty()) {
                        LOG.info("Consumed first Article");
                        printedConsumedArticle = true;
                    }
                    if (!printedConsumedComment && !consumedComments.isEmpty()) {
                        LOG.info("Consumed first Comment");
                        printedConsumedComment = true;
                    }
                    if (!printedConsumedAugmentedArticle && !consumedAugmentedArticles.isEmpty()) {
                        LOG.info("Consumed first AugmentedArticle");
                        printedConsumedAugmentedArticle = true;
                    }
                    if (!printedConsumedAugmentedComment && !consumedAugmentedComments.isEmpty()) {
                        LOG.info("Consumed first AugmentedComment");
                        printedConsumedAugmentedComment = true;
                    }

                    passed = verifySync(
                        false,
                        consumedArticles,
                        consumedComments,
                        consumedAugmentedArticles,
                        consumedAugmentedComments
                    );
                    if (!passed) {
                        LOG.info("Verification has not passed yet. ");
                        Thread.sleep(500);
                    }
                }
                return verifySync(
                    true,
                    consumedArticles,
                    consumedComments,
                    consumedAugmentedArticles,
                    consumedAugmentedComments
                );
            }
        }

        public static void assertThat(final AtomicBoolean pass,
                                      final StringBuilder failures,
                                      final String message,
                                      final boolean passed) {
            if (!passed) {
                if (failures != null) {
                    failures.append("\n").append(message);
                }
                pass.set(false);
            }
        }

        static boolean verifySync(final boolean logResults,
                                  final Map<Integer, Article> consumedArticles,
                                  final Map<Integer, Comment> consumedComments,
                                  final Map<Integer, AugmentedArticle> consumedAugmentedArticles,
                                  final Map<Integer, AugmentedComment> consumedAugmentedComments) {
            final AtomicBoolean pass = new AtomicBoolean(true);
            final StringBuilder report = logResults ? new StringBuilder() : null;

            assertThat(
                pass,
                report,
                "Expected 1 article, got " + consumedArticles.size(),
                consumedArticles.size() > 0
            );
            assertThat(
                pass,
                report,
                "Expected 1 comment, got " + consumedComments.size(),
                consumedComments.size() > 0
            );

            assertThat(
                pass,
                report,
                "Mismatched article size between augmented articles (size "
                    + consumedAugmentedArticles.size() +
                    ") and consumed articles (size "
                    + consumedArticles.size() + ")",
                consumedAugmentedArticles.size() == consumedArticles.size()
            );
            assertThat(
                pass,
                report,
                "Mismatched comments size between augmented comments (size "
                    + consumedAugmentedComments.size() +
                    ") and consumed comments (size " +
                    consumedComments.size() + ")", 
                consumedAugmentedComments.size() == consumedComments.size()
            );

            final Map<Integer, Long> commentCounts = new TreeMap<>();

            for (final RelationalSmokeTest.AugmentedComment augmentedComment : consumedAugmentedComments.values()) {
                final int key = augmentedComment.getKey();
                assertThat(
                    pass,
                    report,
                    "comment missing, but found in augmentedComment: " + key,
                    consumedComments.containsKey(key)
                );

                final Comment comment = consumedComments.get(key);
                if (comment != null) {
                    assertThat(
                        pass,
                        report,
                        "comment missing, but found in augmentedComment: " + key,
                        consumedComments.containsKey(key)
                    );
                }
                commentCounts.put(
                    augmentedComment.getArticleId(),
                    commentCounts.getOrDefault(augmentedComment.getArticleId(), 0L) + 1
                );

                assertThat(
                    pass,
                    report,
                    "augmentedArticle [" + augmentedComment.getArticleId() + "] " +
                        "missing for augmentedComment [" + augmentedComment.getKey() + "]",
                    consumedAugmentedArticles.containsKey(augmentedComment.getArticleId())
                );
                final AugmentedArticle augmentedArticle =
                        consumedAugmentedArticles.get(augmentedComment.getArticleId());
                if (augmentedArticle != null) {
                    assertThat(
                        pass,
                        report,
                        "articlePrefix didn't match augmentedArticle: " + augmentedArticle.getText(),
                        augmentedArticle.getText().startsWith(augmentedComment.getArticlePrefix())
                    );
                }

                assertThat(
                    pass,
                    report,
                    "article " + augmentedComment.getArticleId() + " missing from consumedArticles",
                    consumedArticles.containsKey(augmentedComment.getArticleId())
                );
                final Article article = consumedArticles.get(augmentedComment.getArticleId());
                if (article != null) {
                    assertThat(
                        pass,
                        report,
                        "articlePrefix didn't match article: " + article.getText(),
                        article.getText().startsWith(augmentedComment.getArticlePrefix())
                    );
                }
            }


            for (final RelationalSmokeTest.AugmentedArticle augmentedArticle : consumedAugmentedArticles.values()) {
                assertThat(
                    pass,
                    report,
                    "article " + augmentedArticle.getKey() + " comment count mismatch",
                    augmentedArticle.getCommentCount() == commentCounts.getOrDefault(augmentedArticle.getKey(), 0L)
                );
            }

            if (logResults) {
                if (pass.get()) {
                    LOG.info(
                        "Evaluation passed ({}/{}) articles and ({}/{}) comments",
                        consumedAugmentedArticles.size(),
                        consumedArticles.size(),
                        consumedAugmentedComments.size(),
                        consumedComments.size()
                    );
                } else {
                    LOG.error(
                        "Evaluation failed\nReport: {}\n" +
                            "Consumed Input Articles: {}\n" +
                            "Consumed Input Comments: {}\n" +
                            "Consumed Augmented Articles: {}\n" +
                            "Consumed Augmented Comments: {}",
                        report,
                        consumedArticles,
                        consumedComments,
                        consumedAugmentedArticles,
                        consumedAugmentedComments
                    );
                }
            }

            return pass.get();
        }
    }

    /*
     * Used by the smoke tests.
     */
    public static void main(final String[] args) {
        System.out.println(Arrays.toString(args));
        final String mode = args[0];
        final String kafka = args[1];

        try {
            switch (mode) {
                case "driver": {
                    // this starts the driver (data generation and result verification)
                    final int numArticles = 1_000;
                    final int numComments = 10_000;
                    final DataSet dataSet = DataSet.generate(numArticles, numComments);
                    // publish the data for at least one minute
                    dataSet.produce(kafka, Duration.ofMinutes(1));
                    LOG.info("Smoke test finished producing");
                    // let it soak in
                    Thread.sleep(1000);
                    LOG.info("Smoke test starting verification");
                    // wait for at most 10 minutes to get a passing result
                    final boolean pass = App.verifySync(kafka, Instant.now().plus(Duration.ofMinutes(10)));
                    if (pass) {
                        LOG.info("Smoke test complete: passed");
                    } else {
                        LOG.error("Smoke test complete: failed");
                    }
                    break;
                }
                case "application": {
                    final String nodeId = args[2];
                    final String processingGuarantee = args[3];
                    final String stateDir = args[4];
                    App.startSync(kafka, UUID.randomUUID().toString(), nodeId, processingGuarantee, stateDir);
                    break;
                }
                default:
                    LOG.error("Unknown command: {}", mode);
                    throw new RuntimeException("Unknown command: " + mode);
            }
        } catch (final InterruptedException e) {
            LOG.error("Interrupted", e);
        }
    }
}
