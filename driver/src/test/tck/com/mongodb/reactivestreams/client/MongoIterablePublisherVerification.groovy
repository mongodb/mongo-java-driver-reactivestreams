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

package com.mongodb.reactivestreams.client

import com.mongodb.MongoNamespace
import org.bson.Document
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.AfterClass

import static com.mongodb.reactivestreams.client.Fixture.ObservableSubscriber
import static com.mongodb.reactivestreams.client.Fixture.dropDatabase
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName
import static com.mongodb.reactivestreams.client.Fixture.initializeCollection
import static java.util.concurrent.TimeUnit.SECONDS

class MongoIterablePublisherVerification extends PublisherVerification<Document> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 10000L
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L

    @AfterClass
    void after() {
        dropDatabase(getDefaultDatabaseName())
    }

    MongoIterablePublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS)
    }

    @Override
    Publisher<Document> createPublisher(long elements) {
        assert (elements <= maxElementsFromPublisher())
        def collection = initializeCollection(new MongoNamespace(getDefaultDatabaseName(), getClass().getName()))
        def subscriber = new ObservableSubscriber<Void>()
        collection.insertMany((0..<elements).collect({ new Document('_id', it) })).subscribe(subscriber)
        subscriber.await(10, SECONDS)
        collection.find()
    }

    @Override
    Publisher<Document> createErrorStatePublisher() {
        null
    }

    @Override
    long maxElementsFromPublisher() {
        10000
    }
}
