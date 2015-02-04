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

package com.mongodb.reactivestreams.client

import com.mongodb.MongoNamespace
import com.mongodb.async.client.MongoIterable
import com.mongodb.operation.DistinctOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.ReadPreference.secondary
import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class DistinctPublisherSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = new RootCodecRegistry([new ValueCodecProvider(),
                                               new DocumentCodecProvider(),
                                               new BsonValueCodecProvider()])
    def readPreference = secondary()

    def 'should have the same methods as the wrapped DistinctIterable'() {
        given:
        def wrapped = (com.mongodb.async.client.DistinctIterable.methods*.name - MongoIterable.methods*.name).sort()
        def local = (DistinctPublisher.methods*.name - Publisher.methods*.name - 'batchSize').sort()

        expect:
        wrapped == local
    }

    def 'should build the expected DistinctOperation'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(100) }
        }

        def executor = new TestOperationExecutor([null, null]);
        def wrapped = new com.mongodb.async.client.DistinctIterableImpl<Document>(namespace, Document, codecRegistry, readPreference,
                executor, 'field')
        def disinctPublisher = new DistinctPublisherImpl(wrapped)

        when: 'default input should be as expected'
        disinctPublisher.subscribe(subscriber)

        def operation = executor.getReadOperation() as DistinctOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new DistinctOperation<Document>(namespace, 'field', new DocumentCodec()));
        readPreference == secondary()

        when: 'overriding initial options'
        disinctPublisher.filter(new Document('field', 1)).maxTime(999, MILLISECONDS).subscribe(subscriber)

        operation = executor.getReadOperation() as DistinctOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new DistinctOperation<Document>(namespace, 'field', new DocumentCodec())
                .filter(new BsonDocument('field', new BsonInt32(1)))
                .maxTime(999, MILLISECONDS))
    }

}
