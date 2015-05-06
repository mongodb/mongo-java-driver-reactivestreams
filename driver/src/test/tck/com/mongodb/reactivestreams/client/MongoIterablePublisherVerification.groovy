/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client

import com.mongodb.Block
import com.mongodb.Function
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.MongoIterable
import org.bson.Document
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@SuppressWarnings(['CloseWithoutCloseable', 'UnusedMethodParameter', 'EmptyMethod'])
class MongoIterablePublisherVerification extends PublisherVerification<Document> {

    private ExecutorService pool
    @BeforeClass void before() {
        pool = Executors.newSingleThreadExecutor()
    }
    @AfterClass void after() {
        if (pool != null) {
            pool.shutdown()
        }
    }

    public static final long DEFAULT_TIMEOUT_MILLIS = 10000L
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L

    MongoIterablePublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS)
    }

    @Override
    Publisher<Document> createPublisher(long elements) {
        assert (elements <= maxElementsFromPublisher())
        int batchSize = 1024
        new MongoIterablePublisher<Integer>(new MongoIterable<Integer>() {
            void first(final SingleResultCallback<Integer> callback) { }
            void forEach(final Block<? super Integer> block, final SingleResultCallback<Void> callback) { }
            def <A extends Collection<? super Integer>> void into(final A target, final SingleResultCallback<A> callback) { }
            def <U> MongoIterable<U> map(final Function<Integer, U> mapper) { null }

            @Override
            MongoIterable<Integer> batchSize(final int bSize) { this }

            @Override
            void batchCursor(final SingleResultCallback<AsyncBatchCursor<Integer>> callback) {
                List<Integer> cachedRange = []
                def cachedLength = -1;
                callback.onResult(new AsyncBatchCursor<Integer>() {
                    def totalCount = 0
                    @Override
                    void next(final SingleResultCallback<List<Integer>> batchCallback) {
                        if (totalCount == elements) {
                            batchCallback.onResult(null, null)
                        } else {
                            def start = totalCount + 1
                            def end = ((start + batchSize) <= elements) ? batchSize + totalCount : start + (elements - start)
                            if ((end - start) != cachedLength) {
                                cachedLength = end - start
                                cachedRange = start..end
                            }
                            totalCount = end
                            batchCallback.onResult(cachedRange, null)
                        }
                    }

                    @Override
                    void setBatchSize(final int bSize) { }

                    @Override
                    int getBatchSize() { batchSize }

                    @Override
                    boolean isClosed() { totalCount == elements }

                    @Override
                    void close() { }
                }, null)
            }
        })
    }

    @Override
    Publisher<Document> createFailedPublisher() {
        null
    }

    @Override
    long maxElementsFromPublisher() {
        Integer.MAX_VALUE
    }
}
