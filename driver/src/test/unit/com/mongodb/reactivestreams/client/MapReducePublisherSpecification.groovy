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
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.client.MapReduceIterable
import com.mongodb.async.client.MapReduceIterableImpl
import com.mongodb.async.client.MongoIterable
import com.mongodb.client.model.MapReduceAction
import com.mongodb.operation.FindOperation
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
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
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MapReducePublisherSpecification extends Specification {

        def namespace = new MongoNamespace('db', 'coll')
        def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
        def readPreference = secondary()


    def 'should have the same methods as the wrapped MapReduceIterable'() {
        given:
        def wrapped = (MapReduceIterable.methods*.name - MongoIterable.methods*.name).sort() - 'collation'
        def local = (MapReducePublisher.methods*.name - Publisher.methods*.name - 'batchSize').sort()

        expect:
        wrapped == local
    }

        def 'should build the expected MapReduceWithInlineResultsOperation'() {
            given:
            def subscriber = Stub(Subscriber) {
                onSubscribe(_) >> { args -> args[0].request(100) }
            }
            def executor = new TestOperationExecutor([null, null]);
            def wrapped = new MapReduceIterableImpl<Document, Document>(namespace, Document, Document, codecRegistry, readPreference,
                    ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, executor, 'map', 'reduce')
            def mapReducePublisher = new MapReducePublisherImpl(wrapped)

            when: 'default input should be as expected'
            mapReducePublisher.subscribe(subscriber)

            def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation<Document>
            def readPreference = executor.getReadPreference()

            then:
            expect operation, isTheSameAs(new MapReduceWithInlineResultsOperation<Document>(namespace, new BsonJavaScript('map'),
                    new BsonJavaScript('reduce'), new DocumentCodec()).verbose(true));
            readPreference == secondary()

            when: 'overriding initial options'
            mapReducePublisher.filter(new Document('filter', 1))
                    .finalizeFunction('finalize')
                    .limit(999)
                    .maxTime(999, MILLISECONDS)
                    .scope(new Document('scope', 1))
                    .sort(new Document('sort', 1))
                    .verbose(false)
                    .subscribe(subscriber)

            operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation<Document>

            then: 'should use the overrides'
            expect operation, isTheSameAs(new MapReduceWithInlineResultsOperation<Document>(namespace, new BsonJavaScript('map'),
                    new BsonJavaScript('reduce'), new DocumentCodec())
                    .filter(new BsonDocument('filter', new BsonInt32(1)))
                    .finalizeFunction(new BsonJavaScript('finalize'))
                    .limit(999)
                    .maxTime(999, MILLISECONDS)
                    .scope(new BsonDocument('scope', new BsonInt32(1)))
                    .sort(new BsonDocument('sort', new BsonInt32(1)))
                    .verbose(false)
            )
        }

        def 'should build the expected MapReduceToCollectionOperation'() {
            given:
            def subscriber = Stub(Subscriber) {
                onSubscribe(_) >> { args -> args[0].request(100) }
            }
            def executor = new TestOperationExecutor([null, null, null]);

            when: 'mapReduce to a collection'
            def collectionNamespace = new MongoNamespace('dbName', 'collName')
            def wrapped = new MapReduceIterableImpl<Document, Document>(namespace, Document, Document, codecRegistry, readPreference,
                    ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, executor, 'map', 'reduce')
            def mapReducePublisher = new MapReducePublisherImpl(wrapped)
            mapReducePublisher.collectionName(collectionNamespace.getCollectionName())
                    .databaseName(collectionNamespace.getDatabaseName())
                    .filter(new Document('filter', 1))
                    .finalizeFunction('finalize')
                    .limit(999)
                    .maxTime(999, MILLISECONDS)
                    .scope(new Document('scope', 1))
                    .sort(new Document('sort', 1))
                    .verbose(false)
                    .nonAtomic(true)
                    .action(MapReduceAction.MERGE)
                    .sharded(true)
                    .jsMode(true)
                    .subscribe(subscriber)

            def operation = executor.getWriteOperation() as MapReduceToCollectionOperation
            def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'),
                    new BsonJavaScript('reduce'), 'collName', WriteConcern.ACKNOWLEDGED)
                    .databaseName(collectionNamespace.getDatabaseName())
                    .filter(new BsonDocument('filter', new BsonInt32(1)))
                    .finalizeFunction(new BsonJavaScript('finalize'))
                    .limit(999)
                    .maxTime(999, MILLISECONDS)
                    .scope(new BsonDocument('scope', new BsonInt32(1)))
                    .sort(new BsonDocument('sort', new BsonInt32(1)))
                    .verbose(false)
                    .nonAtomic(true)
                    .action(MapReduceAction.MERGE.getValue())
                    .jsMode(true)
                    .sharded(true)

            then: 'should use the overrides'
            expect operation, isTheSameAs(expectedOperation)

            when: 'the subsequent read should have the batchSize set'
            operation = executor.getReadOperation() as FindOperation<Document>

            then: 'should use the correct settings'
            operation.getNamespace() == collectionNamespace
            operation.getBatchSize() == 100 // set by subscriber.request

            when: 'toCollection should work as expected'
            mapReducePublisher.toCollection().subscribe(subscriber)
            operation = executor.getWriteOperation() as MapReduceToCollectionOperation

            then:
            expect operation, isTheSameAs(expectedOperation)
        }

    }
