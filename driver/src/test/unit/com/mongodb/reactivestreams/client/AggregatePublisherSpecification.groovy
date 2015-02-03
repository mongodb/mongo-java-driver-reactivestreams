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
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
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

class AggregatePublisherSpecification  extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = new RootCodecRegistry([new DocumentCodecProvider(), new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'should have the same methods as the wrapped AggregateIterable'() {
        given:
        def wrapped = (com.mongodb.async.client.AggregateIterable.methods*.name - MongoIterable.methods*.name).sort()
        def local = (AggregatePublisher.methods*.name - Publisher.methods*.name - 'batchSize').sort()

        expect:
        wrapped == local
    }

    def 'should build the expected AggregateOperation'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }

        def executor = new TestOperationExecutor([null, null]);
        def pipeline = [new Document('$match', 1)]
        def wrapped = new com.mongodb.async.client.AggregateIterableImpl<Document>(namespace, Document, codecRegistry, secondary(),
                executor, pipeline)
        def aggregatePublisher = new AggregatePublisherImpl<Document>(wrapped)

        when: 'default input should be as expected'
        aggregatePublisher.subscribe(subscriber)

        def operation = executor.getReadOperation() as AggregateOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec()));
        readPreference == secondary()

        when: 'overriding initial options'
        aggregatePublisher.maxTime(999, MILLISECONDS).useCursor(true).subscribe(subscriber)

        operation = executor.getReadOperation() as AggregateOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec())
                .maxTime(999, MILLISECONDS)
                .useCursor(true))
    }

    def 'should build the expected AggregateToCollectionOperation'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def executor = new TestOperationExecutor([null, null, null, null, null]);
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new Document('$match', 1), new Document('$out', collectionName)]
        def wrapped = new com.mongodb.async.client.AggregateIterableImpl<Document>(namespace, Document, codecRegistry, secondary(),
                executor, pipeline)
        def aggregatePublisher = new AggregatePublisherImpl<Document>(wrapped)
                .batchSize(99)
                .maxTime(999, MILLISECONDS)
                .allowDiskUse(true)
                .useCursor(true)

        when: 'aggregation includes $out'
        aggregatePublisher.subscribe(subscriber)
        def operation = executor.getWriteOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))])
                .maxTime(999, MILLISECONDS)
                .allowDiskUse(true))

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99

        when: 'toCollection should work as expected'
        wrapped = new com.mongodb.async.client.AggregateIterableImpl<Document>(namespace, Document, codecRegistry, secondary(),
                executor, pipeline)
        new AggregatePublisherImpl<Document>(wrapped).toCollection().subscribe(subscriber)
        operation = executor.getWriteOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))]))
    }
}
