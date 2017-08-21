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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;


final class ChangeStreamPublisherImpl<TResult> implements ChangeStreamPublisher<TResult> {

    private final com.mongodb.async.client.ChangeStreamIterable<TResult> wrapped;

    ChangeStreamPublisherImpl(final com.mongodb.async.client.ChangeStreamIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public ChangeStreamPublisher<TResult> fullDocument(final FullDocument fullDocument) {
        wrapped.fullDocument(fullDocument);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> resumeAfter(final Bson resumeToken) {
        wrapped.resumeAfter(resumeToken);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new ObservableToPublisher<TResult>(observe(wrapped)).subscribe(s);
    }
}
