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
import com.mongodb.client.model.MapReduceAction;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;
import static com.mongodb.reactivestreams.client.PublisherHelper.voidToSuccessCallback;


class MapReducePublisherImpl<TResult> implements MapReducePublisher<TResult> {


    private final com.mongodb.async.client.MapReduceIterable<TResult> wrapped;

    MapReducePublisherImpl(final com.mongodb.async.client.MapReduceIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }


    @Override
    public MapReducePublisher<TResult> collectionName(final String collectionName) {
        wrapped.collectionName(collectionName);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> finalizeFunction(final String finalizeFunction) {
        wrapped.finalizeFunction(finalizeFunction);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> scope(final Bson scope) {
        wrapped.scope(scope);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> sort(final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> filter(final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> jsMode(final boolean jsMode) {
        wrapped.jsMode(jsMode);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> verbose(final boolean verbose) {
        wrapped.verbose(verbose);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> action(final MapReduceAction action) {
        wrapped.action(action);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> databaseName(final String databaseName) {
        wrapped.databaseName(databaseName);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> sharded(final boolean sharded) {
        wrapped.sharded(sharded);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> nonAtomic(final boolean nonAtomic) {
        wrapped.nonAtomic(nonAtomic);
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
