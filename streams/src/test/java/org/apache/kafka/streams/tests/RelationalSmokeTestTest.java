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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RelationalSmokeTestTest extends SmokeTestUtil {

    @Test
    public void verifySmokeTestLogic() {
        try (final TopologyTestDriver driver =
                 new TopologyTestDriver(RelationalSmokeTest.App.getTopology(),
                                        RelationalSmokeTest.App.getConfig(
                                            "nothing:0",
                                            "test",
                                            "test",
                                            StreamsConfig.AT_LEAST_ONCE,
                                            TestUtils.tempDirectory().getAbsolutePath()
                                        ))) {

            final TestInputTopic<Integer, RelationalSmokeTest.Article> articles =
                driver.createInputTopic(RelationalSmokeTest.ARTICLE_SOURCE,
                                        new IntegerSerializer(),
                                        new RelationalSmokeTest.Article.ArticleSerializer());

            final TestInputTopic<Integer, RelationalSmokeTest.Comment> comments =
                driver.createInputTopic(RelationalSmokeTest.COMMENT_SOURCE,
                                        new IntegerSerializer(),
                                        new RelationalSmokeTest.Comment.CommentSerializer());

            final TestOutputTopic<Integer, RelationalSmokeTest.AugmentedArticle> augmentedArticles =
                driver.createOutputTopic(RelationalSmokeTest.ARTICLE_RESULT_SINK,
                                         new IntegerDeserializer(),
                                         new RelationalSmokeTest.AugmentedArticle.AugmentedArticleDeserializer());

            final TestOutputTopic<Integer, RelationalSmokeTest.AugmentedComment> augmentedComments =
                driver.createOutputTopic(RelationalSmokeTest.COMMENT_RESULT_SINK,
                                         new IntegerDeserializer(),
                                         new RelationalSmokeTest.AugmentedComment.AugmentedCommentDeserializer());

            final RelationalSmokeTest.DataSet dataSet =
                RelationalSmokeTest.DataSet.generate(10, 30);

            final Map<Integer, RelationalSmokeTest.Article> articleMap = new TreeMap<>();
            for (final RelationalSmokeTest.Article article : dataSet.getArticles()) {
                articles.pipeInput(article.getKey(), article, article.getTimestamp());
                articleMap.put(article.getKey(), article);
            }

            final Map<Integer, Long> commentCounts = new TreeMap<>();

            final Map<Integer, RelationalSmokeTest.Comment> commentMap = new TreeMap<>();
            for (final RelationalSmokeTest.Comment comment : dataSet.getComments()) {
                comments.pipeInput(comment.getKey(), comment, comment.getTimestamp());
                commentMap.put(comment.getKey(), comment);
                commentCounts.put(comment.getArticleId(),
                                  commentCounts.getOrDefault(comment.getArticleId(), 0L) + 1);
            }

            final Map<Integer, RelationalSmokeTest.AugmentedArticle> augmentedArticleResults =
                augmentedArticles.readKeyValuesToMap();

            final Map<Integer, RelationalSmokeTest.AugmentedComment> augmentedCommentResults =
                augmentedComments.readKeyValuesToMap();

            assertThat(augmentedArticleResults.size(), is(dataSet.getArticles().length));
            assertThat(augmentedCommentResults.size(), is(dataSet.getComments().length));

            assertThat(
                RelationalSmokeTest.App.verifySync(true,
                                                   articleMap,
                                                   commentMap,
                                                   augmentedArticleResults,
                                                   augmentedCommentResults),
                is(true));
        }
    }
}