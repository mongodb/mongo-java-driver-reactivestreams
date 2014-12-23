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

import com.mongodb.Function
import org.bson.Document
import org.bson.types.ObjectId

import static Fixture.ObservableSubscriber
import static java.util.concurrent.TimeUnit.SECONDS

class MapSpecification extends FunctionalSpecification {

    def documents = [new Document('_id', new ObjectId()).append('x', 42),
                     new Document('_id', new ObjectId()).append('x', 43)]

    def setup() {
        def subscriber = new ObservableSubscriber<Void>()
        collection.insertMany(documents).subscribe(subscriber)
        subscriber.await(10, SECONDS)
    }

    def 'should map source document into target document with into'() {
        expect:
        def subscriber = new ObservableSubscriber<TargetDocument>();
        Publishers.map(collection.find(new Document()), new MappingFunction()).subscribe(subscriber)
        subscriber.get(10, SECONDS) == [new TargetDocument(documents[0]), new TargetDocument(documents[1])]
    }

    def 'should map when already mapped'() {
        when:
        def subscriber = new ObservableSubscriber<ObjectId>();
        Publishers.map(Publishers.map(collection.find(new Document()),
                new MappingFunction()),
                new Function<TargetDocument, ObjectId>() {
                    @Override
                    ObjectId apply(final TargetDocument targetDocument) {
                        targetDocument.getId()
                    }
                }).subscribe(subscriber)

        then:
        subscriber.get(10, SECONDS) == [new TargetDocument(documents[0]).getId(), new TargetDocument(documents[1]).getId()]
    }

    static class MappingFunction implements Function<Document, TargetDocument> {
        @Override
        TargetDocument apply(final Document document) {
            new TargetDocument(document)
        }
    }
}