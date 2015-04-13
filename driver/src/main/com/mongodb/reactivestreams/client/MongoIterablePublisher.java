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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoIterable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class MongoIterablePublisher<TResult> implements Publisher<TResult> {

    private final MongoIterable<TResult> mongoIterable;

    MongoIterablePublisher(final MongoIterable<TResult> mongoIterable) {
        this.mongoIterable = mongoIterable;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new AsyncBatchCursorSubscription(s).start();
    }

    private class AsyncBatchCursorSubscription extends SubscriptionSupport<TResult> {
        private final AtomicBoolean requestedBatchCursorLock = new AtomicBoolean();
        private final AtomicBoolean bufferProcessingLock = new AtomicBoolean();
        private final AtomicBoolean batchCursorNextLock = new AtomicBoolean();
        private final AtomicBoolean cursorCompleted = new AtomicBoolean();
        private final AtomicReference<AsyncBatchCursor<TResult>> batchCursor = new AtomicReference<AsyncBatchCursor<TResult>>();
        private final AtomicLong wanted = new AtomicLong();
        private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

        public AsyncBatchCursorSubscription(final Subscriber<? super TResult> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            wanted.addAndGet(n);
            if (requestedBatchCursorLock.compareAndSet(false, true)) {
                if (n <= 1) {
                    mongoIterable.batchSize(2);
                } else if (n < Integer.MAX_VALUE) {
                    mongoIterable.batchSize((int) n);
                } else {
                    mongoIterable.batchSize(Integer.MAX_VALUE);
                }
                mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
                    @Override
                    public void onResult(final AsyncBatchCursor<TResult> result, final Throwable t) {
                        if (t != null) {
                            onError(t);
                        } else if (result != null) {
                            batchCursor.set(result);
                            getNextBatch();
                        } else {
                            onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                        }
                    }
                });
            } else if (batchCursor.get() != null) { // we have the batch cursor so start to process the resultsQueue
                processResultsQueue();
            }
        }

        @Override
        protected void handleCancel() {
            super.handleCancel();
            AsyncBatchCursor<TResult> cursor = batchCursor.get();
            if (cursor != null) {
                cursor.close();
            }
        }

        void getNextBatch() {
            log("getNextBatch");
            if (batchCursorNextLock.compareAndSet(false, true)) {
                final AsyncBatchCursor<TResult> cursor = batchCursor.get();
                if (cursor.isClosed()) {
                    cursorCompleted.set(true);
                    batchCursorNextLock.set(false);
                    processResultsQueue();
                } else {
                    int batchSize = wanted.get() > Integer.MAX_VALUE ? Integer.MAX_VALUE : wanted.intValue();
                    cursor.setBatchSize(batchSize);
                    cursor.next(new SingleResultCallback<List<TResult>>() {
                        @Override
                        public void onResult(final List<TResult> result, final Throwable t) {
                            if (t != null) {
                                onError(t);
                                batchCursorNextLock.set(false);
                            } else {
                                if (result != null) {
                                    resultsQueue.addAll(result);
                                } else {
                                    cursorCompleted.set(true);
                                }
                                batchCursorNextLock.set(false);
                                processResultsQueue();
                            }
                        }
                    });
                }
            }
        }

        /**
         * Original implementation from RatPack
         */
        void processResultsQueue() {
            if (bufferProcessingLock.compareAndSet(false, true)) {
                try {
                    long i = wanted.get();
                    while (i > 0) {
                        TResult item = resultsQueue.poll();
                        if (item == null) {
                            // Nothing left to process
                            break;
                        } else {
                            onNext(item);
                            i = wanted.decrementAndGet();
                        }
                    }
                    if (cursorCompleted.get()) {
                        onComplete();  // Cursor has completed and there are no more items left to process
                    }
                } finally {
                    bufferProcessingLock.set(false);
                }

                if (!cursorCompleted.get() && wanted.get() > resultsQueue.size()) {
                    getNextBatch();
                } else if (resultsQueue.peek() != null) {
                    if (wanted.get() > 0) {
                        processResultsQueue();
                    } else if (cursorCompleted.get()) {
                        onComplete();
                    }
                }
            }
        }
    }

}
