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

package com.mongodb.reactivestreams.client;

import com.mongodb.async.client.MongoClientSettings;
import org.bson.Document;
import org.reactivestreams.Publisher;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;

class MongoClientImpl implements MongoClient {
    private final com.mongodb.async.client.MongoClient wrapped;

    MongoClientImpl(final com.mongodb.async.client.MongoClient wrapped) {
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
    public MongoClientSettings getSettings() {
        return wrapped.getSettings();
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return new ObservableToPublisher<String>(observe(wrapped.listDatabaseNames()));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(final Class<TResult> clazz) {
        return new ListDatabasesPublisherImpl<TResult>(wrapped.listDatabases(clazz));
    }
}
