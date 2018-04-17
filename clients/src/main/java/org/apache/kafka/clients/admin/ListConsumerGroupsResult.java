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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * The result of the {@link AdminClient#listConsumerGroups()} call.
 * <p>
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsResult {
    private final Map<Node, KafkaFutureImpl<Collection<ConsumerGroupListing>>> futuresMap;
    private final KafkaFuture<Collection<ConsumerGroupListing>> flattenFuture;
    private final KafkaFuture<Void> listFuture;

    ListConsumerGroupsResult(final KafkaFuture<Void> listFuture,
                             final KafkaFuture<Collection<ConsumerGroupListing>> flattenFuture,
                             final Map<Node, KafkaFutureImpl<Collection<ConsumerGroupListing>>> futuresMap) {
        this.flattenFuture = flattenFuture;
        this.listFuture = listFuture;
        this.futuresMap = futuresMap;
    }

    private class FutureConsumerGroupListingIterator extends AbstractIterator<KafkaFuture<ConsumerGroupListing>> {
        private Iterator<KafkaFutureImpl<Collection<ConsumerGroupListing>>> futuresIter;
        private Iterator<ConsumerGroupListing> innerIter;

        @Override
        protected KafkaFuture<ConsumerGroupListing> makeNext() {
            if (futuresIter == null) {
                try {
                    listFuture.get();
                } catch (Exception e) {
                    // the list future has failed, there will be no listings to show at all
                    return allDone();
                }

                futuresIter = futuresMap.values().iterator();
            }

            while (innerIter == null || !innerIter.hasNext()) {
                if (futuresIter.hasNext()) {
                    KafkaFuture<Collection<ConsumerGroupListing>> collectionFuture = futuresIter.next();
                    try {
                        Collection<ConsumerGroupListing> collection = collectionFuture.get();
                        innerIter = collection.iterator();
                    } catch (Exception e) {
                        KafkaFutureImpl<ConsumerGroupListing> future = new KafkaFutureImpl<>();
                        future.completeExceptionally(e);
                        return future;
                    }
                } else {
                    return allDone();
                }
            }

            KafkaFutureImpl<ConsumerGroupListing> future = new KafkaFutureImpl<>();
            future.complete(innerIter.next());
            return future;
        }
    }

    /**
     * Return an iterator of futures for ConsumerGroupListing objects; the returned future will throw exception
     * if we cannot get a complete collection of consumer listings.
     */
    public Iterator<KafkaFuture<ConsumerGroupListing>> iterator() {
        return new FutureConsumerGroupListingIterator();
    }

    /**
     * Return a future which yields a full collection of ConsumerGroupListing objects; will throw exception
     * if we cannot get a complete collection of consumer listings.
     */
    public KafkaFuture<Collection<ConsumerGroupListing>> all() {
        return flattenFuture;
    }
}
