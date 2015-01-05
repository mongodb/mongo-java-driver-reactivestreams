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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class FlattenPublisher<T> implements Publisher<T> {

    private final Publisher<List<T>> input;

    public FlattenPublisher(final Publisher<List<T>> publisher) {
        this.input = publisher;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        new FlattenSubscription(subscriber).start();
    }

    private class FlattenSubscription extends SubscriptionSupport<T> {

        private final AtomicBoolean inputFinished = new AtomicBoolean();
        private final AtomicReference<Subscription> inputSubscription = new AtomicReference<Subscription>();
        private final AtomicLong wanted = new AtomicLong();
        private final ConcurrentLinkedQueue<T> buffer = new ConcurrentLinkedQueue<T>();
        private final AtomicBoolean draining = new AtomicBoolean();

        public FlattenSubscription(final Subscriber<? super T> subscriber) {
            super(subscriber);
            input.subscribe(new Subscriber<List<T>>() {
                @Override
                public void onSubscribe(final Subscription s) {
                    inputSubscription.set(s);
                }

                @Override
                public void onNext(final List<T> results) {
                    buffer.addAll(results);
                    tryDrain();
                }

                @Override
                public void onError(final Throwable t) {
                    FlattenSubscription.this.onError(t);
                    inputFinished.set(true);
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
            tryDrain();
        }

        @Override
        protected void handleCancel() {
            super.handleCancel();
            Subscription subscription = inputSubscription.get();
            if (subscription != null) {
                subscription.cancel();
            }
        }

        /**
         * Original implementation from RatPack
         */
        public void tryDrain() {
            if (draining.compareAndSet(false, true)) {
                try {
                    long i = wanted.get();
                    while (i > 0) {
                        T item = buffer.poll();
                        if (item == null) {
                            if (inputFinished.get()) {
                                onComplete();
                                return;
                            } else {
                                inputSubscription.get().request(1);
                            }
                        } else {
                            onNext(item);
                            i = wanted.decrementAndGet();
                        }
                    }

                    if (inputFinished.get() && buffer.peek() == null) {
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

