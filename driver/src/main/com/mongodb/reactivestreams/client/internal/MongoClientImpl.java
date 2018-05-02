/*
 * Copyright 2014-2015 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.ClientSessionOptions;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.session.ClientSession;
import org.bson.Document;
import org.reactivestreams.Publisher;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;

/**
 * The internal MongoClient implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class MongoClientImpl implements MongoClient {
    private final com.mongodb.async.client.MongoClient wrapped;

    /**
     * The internal MongoClientImpl constructor.
     *
     * <p>This should not be considered a part of the public API.</p>
     * @param wrapped the underlying MongoClient
     */
    public MongoClientImpl(final com.mongodb.async.client.MongoClient wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(wrapped.getDatabase(name));
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    @Deprecated
    public com.mongodb.async.client.MongoClientSettings getSettings() {
        return wrapped.getSettings();
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return new ObservableToPublisher<String>(observe(wrapped.listDatabaseNames()));
    }

    @Override
    public Publisher<String> listDatabaseNames(final ClientSession clientSession) {
        return new ObservableToPublisher<String>(observe(wrapped.listDatabaseNames(clientSession)));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(final Class<TResult> clazz) {
        return new ListDatabasesPublisherImpl<TResult>(wrapped.listDatabases(clazz));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> clazz) {
        return new ListDatabasesPublisherImpl<TResult>(wrapped.listDatabases(clientSession, clazz));
    }

    @Override
    public Publisher<ClientSession> startSession(final ClientSessionOptions options) {
        return new ObservableToPublisher<ClientSession>(observe(new Block<SingleResultCallback<ClientSession>>() {
            @Override
            public void apply(final SingleResultCallback<ClientSession> clientSessionSingleResultCallback) {
                wrapped.startSession(options, clientSessionSingleResultCallback);
            }
        }));
    }
}
