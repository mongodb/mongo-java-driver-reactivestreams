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

import com.mongodb.CursorType;
import com.mongodb.async.SingleResultCallback;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class FindPublisherImpl<T> implements FindPublisher<T> {

    private final com.mongodb.async.client.FindIterable<T> wrapped;

    FindPublisherImpl(final com.mongodb.async.client.FindIterable<T> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public Publisher<T> first() {
        return new SingleResultPublisher<T>() {
            @Override
            void execute(final SingleResultCallback<T> callback) {
                wrapped.first(callback);
            }
        };
    }

    @Override
    public FindPublisher<T> filter(final Object filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public FindPublisher<T> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public FindPublisher<T> skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public FindPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindPublisher<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public FindPublisher<T> modifiers(final Object modifiers) {
        wrapped.modifiers(modifiers);
        return this;
    }

    @Override
    public FindPublisher<T> projection(final Object projection) {
        wrapped.projection(projection);
        return this;
    }

    @Override
    public FindPublisher<T> sort(final Object sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public FindPublisher<T> noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindPublisher<T> oplogReplay(final boolean oplogReplay) {
        wrapped.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindPublisher<T> partial(final boolean partial) {
        wrapped.partial(partial);
        return this;
    }

    @Override
    public FindPublisher<T> cursorType(final CursorType cursorType) {
        wrapped.cursorType(cursorType);
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super T> s) {
        new MongoIterablePublisher<T>(wrapped).subscribe(s);
    }
}
