/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.Document
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment

import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient

class ReadOperationPublisherVerification extends PublisherVerification<Document> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 10000L
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L

    ReadOperationPublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS)
    }

    @Override
    Publisher<Document> createPublisher(long elements) {
        assert (elements <= maxElementsFromPublisher())
        getMongoClient().getDatabase('admin').executeCommand(new Document('ping', 1), primaryPreferred())
    }

    @Override
    Publisher<Document> createErrorStatePublisher() {
        null
    }

    @Override
    long maxElementsFromPublisher() {
        1
    }
}