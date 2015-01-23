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

class MongoIterablePublisher<T> implements Publisher<T> {

    private final MongoIterable<T> mongoIterable;

    MongoIterablePublisher(final MongoIterable<T> mongoIterable) {
        this.mongoIterable = mongoIterable;
    }

    @Override
    public void subscribe(final Subscriber<? super T> s) {
        new AsyncBatchCursorSubscription(s).start();
    }

    private class AsyncBatchCursorSubscription extends SubscriptionSupport<T> {
        private final AtomicBoolean operationCompleted = new AtomicBoolean();
        private final AtomicReference<AsyncBatchCursor<T>> batchCursor = new AtomicReference<AsyncBatchCursor<T>>();
        private final AtomicLong wanted = new AtomicLong();
        private final ConcurrentLinkedQueue<T> buffer = new ConcurrentLinkedQueue<T>();
        private final AtomicBoolean draining = new AtomicBoolean();
        private final AtomicBoolean getMore = new AtomicBoolean();

        public AsyncBatchCursorSubscription(final Subscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            wanted.addAndGet(n);
            if (operationCompleted.compareAndSet(false, true)) {
                execute();
            } else if (batchCursor.get() != null) {
                tryDrain();
                if (wanted.get() > buffer.size()) {
                    getNextBatch();
                }
            }
        }

        @Override
        protected void handleCancel() {
            super.handleCancel();
            AsyncBatchCursor<T> cursor = batchCursor.get();
            if (cursor != null) {
                cursor.close();
            }
        }

        void execute(){
            mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<T>>() {
                @Override
                public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
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
        }

        void getNextBatch() {
            log("getNextBatch");
            if (getMore.compareAndSet(false, true)) {
                AsyncBatchCursor<T> cursor = batchCursor.get();
                if (cursor.isClosed()) {
                    getMore.set(false);
                    tryDrain();
                } else {
                    cursor.next(new SingleResultCallback<List<T>>() {
                        @Override
                        public void onResult(final List<T> result, final Throwable t) {
                            if (t != null) {
                                onError(t);
                                getMore.set(false);
                            } else {
                                if (result != null) {
                                    buffer.addAll(result);
                                }
                                getMore.set(false);
                                tryDrain();
                            }
                        }
                    });
                }
            }
        }

        /**
         * Original implementation from RatPack
         */
        void tryDrain() {
            if (draining.compareAndSet(false, true)) {
                try {
                    long i = wanted.get();
                    while (i > 0) {
                        T item = buffer.poll();
                        if (item == null) {
                            // Nothing left to process
                            break;
                        } else {
                            onNext(item);
                            i = wanted.decrementAndGet();
                        }
                    }
                    AsyncBatchCursor<T> cursor = batchCursor.get();
                    if (cursor != null && cursor.isClosed()) {
                        onComplete();
                    }
                } finally {
                    draining.set(false);
                }
                if (buffer.peek() != null && wanted.get() > 0) {
                    tryDrain();
                }
            }
        }
    }

}
