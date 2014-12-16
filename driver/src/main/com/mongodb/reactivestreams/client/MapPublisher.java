/*
 * Copyright 2014 the original author or authors (http://www.ratpack.io)
 * Copyright 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Original implementation from RatPack
 */
class MapPublisher<I, O> implements MongoPublisher<O> {

    private final Publisher<I> input;
    private final Function<? super I, ? extends O> function;

    public MapPublisher(final Publisher<I> input, final Function<? super I, ? extends O> function) {
        this.input = input;
        this.function = function;
    }

    @Override
    public void subscribe(final Subscriber<? super O> subscriber) {
        input.subscribe(new Subscriber<I>() {

            private final AtomicBoolean done = new AtomicBoolean();
            private Subscription subscription;

            @Override
            public void onSubscribe(final Subscription subscription) {
                this.subscription = subscription;
                subscriber.onSubscribe(this.subscription);
            }

            @Override
            public void onNext(final I in) {
                O out;
                try {
                    out = function.apply(in);
                } catch (Throwable throwable) {
                    subscription.cancel();
                    onError(throwable);
                    return;
                }
                if (!done.get()) {
                    subscriber.onNext(out);
                }
            }

            @Override
            public void onError(final Throwable t) {
                if (done.compareAndSet(false, true)) {
                    subscriber.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (done.compareAndSet(false, true)) {
                    subscriber.onComplete();
                }
            }
        });
    }

    @Override
    public <T> MongoPublisher<T> map(final Function<? super O, ? extends T> function) {
        return Publishers.map(this, function);
    }
}
