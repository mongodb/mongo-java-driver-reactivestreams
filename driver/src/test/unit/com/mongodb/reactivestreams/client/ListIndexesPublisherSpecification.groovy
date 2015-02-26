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
import com.mongodb.async.client.ListIndexesIterableImpl
import com.mongodb.async.client.MongoIterable
import com.mongodb.operation.ListIndexesOperation
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.ReadPreference.secondary
import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistryHelper.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class ListIndexesPublisherSpecification extends Specification {

    def 'should have the same methods as the wrapped ListIndexesIterable'() {
        given:
        def wrapped = (com.mongodb.async.client.ListIndexesIterable.methods*.name - MongoIterable.methods*.name).sort()
        def local = (ListIndexesPublisher.methods*.name - Publisher.methods*.name - 'batchSize').sort()

        expect:
        wrapped == local
    }

    def 'should build the expected listIndexesOperation'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(100) }
        }
        def namespace = new MongoNamespace('db', 'coll')
        def codecRegistry = fromProviders([new DocumentCodecProvider(), new BsonValueCodecProvider(), new ValueCodecProvider()])
        def executor = new TestOperationExecutor([null, null]);
        def wrapped = new ListIndexesIterableImpl(namespace, Document, codecRegistry, secondary(), executor)
        def listIndexesPublisher = new ListIndexesPublisherImpl<Document>(wrapped)

        when: 'default input should be as expected'
        listIndexesPublisher.subscribe(subscriber)

        def operation = executor.getReadOperation() as ListIndexesOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ListIndexesOperation<Document>(namespace, new DocumentCodec()).batchSize(100))
        readPreference == secondary()

        when: 'overriding initial options'
        listIndexesPublisher
                .maxTime(999, MILLISECONDS)
                .subscribe(subscriber)

        operation = executor.getReadOperation() as ListIndexesOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ListIndexesOperation<Document>(namespace, new DocumentCodec())
                .batchSize(100).maxTime(999, MILLISECONDS))
    }

}
