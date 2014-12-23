/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 * Copyright 2014 the original author or authors (http://www.ratpack.io)
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

import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class AsyncBatchCursorPublisher<T> implements Publisher<List<T>> {
    private final Publisher<? extends AsyncBatchCursor<T>> input;

    public AsyncBatchCursorPublisher(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation, final ReadPreference readPreference,
                                     final AsyncOperationExecutor executor) {
        this.input = Publishers.publish(operation, readPreference, executor);
    }

    @Override
    public void subscribe(final Subscriber<? super List<T>> s) {
        new AsyncBatchCursorSubscription(s).start();
    }

    private class AsyncBatchCursorSubscription extends SubscriptionSupport<List<T>> {
        private final AtomicReference<AsyncBatchCursor<T>> batchCursor = new AtomicReference<AsyncBatchCursor<T>>();
        private final AtomicBoolean inputRequested = new AtomicBoolean();
        private final AtomicBoolean inputFinished = new AtomicBoolean();
        private final AtomicReference<Subscription> inputSubscription = new AtomicReference<Subscription>();
        private final AtomicLong wanted = new AtomicLong();
        private final ConcurrentLinkedQueue<List<T>> buffer = new ConcurrentLinkedQueue<List<T>>();
        private final AtomicBoolean draining = new AtomicBoolean();
        private final AtomicBoolean getMore = new AtomicBoolean();

        public AsyncBatchCursorSubscription(final Subscriber<? super List<T>> subscriber) {
            super(subscriber);
            input.subscribe(new Subscriber<AsyncBatchCursor<T>>() {

                @Override
                public void onSubscribe(final Subscription s) {
                    inputSubscription.set(s);
                }

                @Override
                public void onNext(final AsyncBatchCursor<T> cursor) {
                    batchCursor.set(cursor);
                    getNextBatch();
                }

                @Override
                public void onError(final Throwable t) {
                    inputFinished.set(true);
                    AsyncBatchCursorSubscription.this.onError(t);
                }

                @Override
                public void onComplete() {
                    inputFinished.set(true);
                }
            });
        }

        @Override
        protected void doRequest(final long n) {
            wanted.addAndGet(n);
            if (inputRequested.compareAndSet(false, true)) {
                inputSubscription.get().request(1);
            } else if (inputFinished.get()) {
                tryDrain();
                if (buffer.size() < wanted.get()) {
                    getNextBatch();
                }
            }
        }

        void getNextBatch() {
            final AsyncBatchCursor<T> cursor = batchCursor.get();
            if (cursor != null && getMore.compareAndSet(false, true)) {
                if (cursor.isClosed()) {
                    inputFinished.set(true);
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
                                    buffer.add(result);
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
                        List<T> item = buffer.poll();
                        if (item == null) {
                            if (inputFinished.get()) {
                                onComplete();
                                return;
                            } else {
                                break;
                            }
                        } else {
                            onNext(item);
                            i = wanted.decrementAndGet();
                        }
                    }
                    if (buffer.peek() != null && wanted.get() > 0) {
                        tryDrain();
                    } else if (inputFinished.get() && buffer.peek() == null) {
                        onComplete();
                    }
                } finally {
                    draining.set(false);
                }
            }
        }
    }
}
