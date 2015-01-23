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

import com.mongodb.async.client.MongoIterable
import com.mongodb.operation.ListCollectionsOperation
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

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class ListCollectionsFluentSpecification extends Specification {

    def 'should have the same methods as the wrapped ListCollectionsFluent'() {
        given:
        def wrapped = (com.mongodb.async.client.ListCollectionsFluent.methods*.name - MongoIterable.methods*.name).sort()
        def local = (ListCollectionsFluent.methods*.name - Publisher.methods*.name).sort()

        expect:
        wrapped == local
    }

    def 'should build the expected listCollectionOperation'() {
        given:
        def codecRegistry = new RootCodecRegistry([new DocumentCodecProvider(), new BsonValueCodecProvider(), new ValueCodecProvider()])
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def executor = new TestOperationExecutor([null, null]);
        def wrapped = new com.mongodb.async.client.ListCollectionsFluentImpl('db', Document, codecRegistry, secondary(), executor)
        def listCollectionFluent = new ListCollectionsFluentImpl<Document>(wrapped)

        when: 'default input should be as expected'
        listCollectionFluent.subscribe(subscriber)

        def operation = executor.getReadOperation() as ListCollectionsOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation<Document>('db', new DocumentCodec()))
        readPreference == secondary()

        when: 'overriding initial options'
        listCollectionFluent.filter(new Document('filter', 2)).batchSize(99).maxTime(999, MILLISECONDS).subscribe(subscriber)

        operation = executor.getReadOperation() as ListCollectionsOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ListCollectionsOperation<Document>('db', new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2))).batchSize(99).maxTime(999, MILLISECONDS))
    }

}
