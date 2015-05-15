/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;
import static com.mongodb.reactivestreams.client.PublisherHelper.voidToSuccessCallback;

class AggregatePublisherImpl<TResult> implements AggregatePublisher<TResult> {

    private final com.mongodb.async.client.AggregateIterable<TResult> wrapped;

    AggregatePublisherImpl(final com.mongodb.async.client.AggregateIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }


    @Override
    public AggregatePublisher<TResult> allowDiskUse(final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> useCursor(final Boolean useCursor) {
        wrapped.useCursor(useCursor);
        return this;
    }

    @Override
    public Publisher<Success> toCollection() {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>(){
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.toCollection(voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new ObservableToPublisher<TResult>(observe(wrapped)).subscribe(s);
    }
}
