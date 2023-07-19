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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.function.Function;

/**
 * The {@code TableJoined} class represents optional parameters that can be passed to
 * {@link KTable#join(KTable, Function, ValueJoiner, TableJoined) KTable#join(KTable,Function,...)} and
 * {@link KTable#leftJoin(KTable, Function, ValueJoiner, TableJoined) KTable#leftJoin(KTable,Function,...)}
 * operations, for foreign key joins.
 * @param <K>   this key type ; key type for the left (primary) table
 * @param <KO>  other key type ; key type for the right (foreign key) table
 */
public class TableJoined<K, KO> implements NamedOperation<TableJoined<K, KO>> {

    protected final StreamPartitioner<K, Void> partitioner;
    protected final StreamPartitioner<KO, Void> otherPartitioner;
    protected final String name;

    private TableJoined(final StreamPartitioner<K, Void> partitioner,
                        final StreamPartitioner<KO, Void> otherPartitioner,
                        final String name) {
        this.partitioner = partitioner;
        this.otherPartitioner = otherPartitioner;
        this.name = name;
    }

    protected TableJoined(final TableJoined<K, KO> tableJoined) {
        this(tableJoined.partitioner, tableJoined.otherPartitioner, tableJoined.name);
    }

    /**
     * Create an instance of {@code TableJoined} with partitioner and otherPartitioner {@link StreamPartitioner} instances.
     * {@code null} values are accepted and will result in the default partitioner being used.
     *
     * @param partitioner      a {@link StreamPartitioner} that captures the partitioning strategy for the left (primary)
     *                         table of the foreign key join. Specifying this option does not repartition or otherwise
     *                         affect the source table; rather, this option informs the foreign key join on how internal
     *                         topics should be partitioned in order to be co-partitioned with the left join table.
     *                         The partitioning strategy must depend only on the message key and not the message value,
     *                         else the source table is not supported with foreign key joins. This option may be left
     *                         {@code null} if the source table uses the default partitioner.
     * @param otherPartitioner a {@link StreamPartitioner} that captures the partitioning strategy for the right (foreign
     *                         key) table of the foreign key join. Specifying this option does not repartition or otherwise
     *                         affect the source table; rather, this option informs the foreign key join on how internal
     *                         topics should be partitioned in order to be co-partitioned with the right join table.
     *                         The partitioning strategy must depend only on the message key and not the message value,
     *                         else the source table is not supported with foreign key joins. This option may be left
     *                         {@code null} if the source table uses the default partitioner.
     * @param <K>              this key type ; key type for the left (primary) table
     * @param <KO>             other key type ; key type for the right (foreign key) table
     * @return new {@code TableJoined} instance with the provided partitioners
     */
    public static <K, KO> TableJoined<K, KO> with(final StreamPartitioner<K, Void> partitioner,
                                                  final StreamPartitioner<KO, Void> otherPartitioner) {
        return new TableJoined<>(partitioner, otherPartitioner, null);
    }

    /**
     * Create an instance of {@code TableJoined} with base name for all components of the join, including internal topics
     * created to complete the join.
     *
     * @param name the name used as the base for naming components of the join including internal topics
     * @param <K>  this key type ; key type for the left (primary) table
     * @param <KO> other key type ; key type for the right (foreign key) table
     * @return new {@code TableJoined} instance configured with the {@code name}
     *
     */
    public static <K, KO> TableJoined<K, KO> as(final String name) {
        return new TableJoined<>(null, null, name);
    }

    /**
     * Set the custom {@link StreamPartitioner} to be used as part of computing the join.
     * {@code null} values are accepted and will result in the default partitioner being used.
     *
     * @param partitioner a {@link StreamPartitioner} that captures the partitioning strategy for the left (primary)
     *                    table of the foreign key join. Specifying this option does not repartition or otherwise
     *                    affect the source table; rather, this option informs the foreign key join on how internal
     *                    topics should be partitioned in order to be co-partitioned with the left join table.
     *                    The partitioning strategy must depend only on the message key and not the message value,
     *                    else the source table is not supported with foreign key joins. This option may be left
     *                    {@code null} if the source table uses the default partitioner.
     * @return new {@code TableJoined} instance configured with the {@code partitioner}
     */
    public TableJoined<K, KO> withPartitioner(final StreamPartitioner<K, Void> partitioner) {
        return new TableJoined<>(partitioner, otherPartitioner, name);
    }

    /**
     * Set the custom other {@link StreamPartitioner} to be used as part of computing the join.
     * {@code null} values are accepted and will result in the default partitioner being used.
     *
     * @param otherPartitioner a {@link StreamPartitioner} that captures the partitioning strategy for the right (foreign
     *                         key) table of the foreign key join. Specifying this option does not repartition or otherwise
     *                         affect the source table; rather, this option informs the foreign key join on how internal
     *                         topics should be partitioned in order to be co-partitioned with the right join table.
     *                         The partitioning strategy must depend only on the message key and not the message value,
     *                         else the source table is not supported with foreign key joins. This option may be left
     *                         {@code null} if the source table uses the default partitioner.
     * @return new {@code TableJoined} instance configured with the {@code otherPartitioner}
     */
    public TableJoined<K, KO> withOtherPartitioner(final StreamPartitioner<KO, Void> otherPartitioner) {
        return new TableJoined<>(partitioner, otherPartitioner, name);
    }

    /**
     * Set the base name used for all components of the join, including internal topics
     * created to complete the join.
     *
     * @param name the name used as the base for naming components of the join including internal topics
     * @return new {@code TableJoined} instance configured with the {@code name}
     */
    @Override
    public TableJoined<K, KO> withName(final String name) {
        return new TableJoined<>(partitioner, otherPartitioner, name);
    }
}
