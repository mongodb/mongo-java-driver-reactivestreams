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

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.WriteConcernResult
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DistinctOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.MapReduceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexOperation
import com.mongodb.operation.DeleteOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.DropCollectionOperation
import com.mongodb.operation.DropIndexOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.ListIndexesOperation
import com.mongodb.operation.MapReduceStatistics
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.RenameCollectionOperation
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.bulk.BulkWriteResult.acknowledged
import static com.mongodb.bulk.BulkWriteResult.unacknowledged
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE
import static Fixture.ObservableSubscriber
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings('ClassSize')
class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def options = OperationOptions.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(secondary())
            .codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()
    def getOptions = { WriteConcern writeConcern ->
        OperationOptions.builder().writeConcern(writeConcern).build().withDefaults(options)
    }
    def asyncDocCursor = {
        Stub(AsyncBatchCursor) {
            def seen = false;
            next(_) >> { args ->
                if (seen) {
                    args[0].onResult([new Document('c', 1)], null)
                } else {
                    args[0].onResult([new Document('a', 1), new Document('b', 1)], null)
                    seen = true
                }

            }
        }
    }

    def 'should return the correct name from getName'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should return the correct options'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getOptions() == options
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CountOperation(namespace).filter(filter)
        def subscriber = new ObservableSubscriber<Long>();

        when:
        collection.count().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [1L]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        subscriber = new ObservableSubscriber<Long>()
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.count(filter).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [2L]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        subscriber = new ObservableSubscriber<Long>()
        def hint = new BsonDocument('hint', new BsonInt32(1))
        collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS)).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [3L]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray([new BsonString('a'), new BsonInt32(1)]),
                                                  new BsonArray([new BsonString('c')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Object>()

        when:
        collection.distinct('test', filter).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == ['a']
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == ['a', 1]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(new BsonDocument()))

        when:
        subscriber = new ObservableSubscriber<Object>()
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.distinct('test', filter, new DistinctOptions().maxTime(100, MILLISECONDS)).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == ['c']
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(filter).maxTime(100, MILLISECONDS))
    }

    def 'should handle exceptions in distinct correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure'),
                                                  new BsonArray([new BsonString('no document codec')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Object>()

        when: 'A failed operation'
        collection.distinct('test', filter).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        notThrown(MongoException)
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().size() == 1

        when: 'An unexpected result'
        subscriber = new ObservableSubscriber<Object>()
        collection.distinct('test', filter).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        notThrown(MongoException)
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().size() == 1

        when: 'A missing codec should throw immediately'
        collection.distinct('test', new Document())

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use FindOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([asyncDocCursor(), asyncDocCursor(), asyncDocCursor(), asyncDocCursor()])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new FindOperation(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def bsonOperation = new FindOperation(namespace, new BsonDocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.find().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1), new Document('c', 1)]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.find(new Document('filter', 1)).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        subscriber = new ObservableSubscriber<BsonDocument>()
        collection.find(BsonDocument).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        subscriber = new ObservableSubscriber<BsonDocument>()
        collection.find(new Document('filter', 1), BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))
    }

    def 'should use AggregateOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([asyncDocCursor(), asyncDocCursor(), asyncDocCursor()])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.aggregate([new Document('$match', 1)]).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1), new Document('c', 1)]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec()))

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.aggregate([new Document('$match', 1)], new AggregateOptions().maxTime(100, MILLISECONDS)).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec()).maxTime(100, MILLISECONDS))

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.aggregate([new Document('$match', 1)], BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new BsonDocumentCodec()))
    }

    def 'should use AggregateToCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, asyncDocCursor()])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.aggregate([new Document('$out', 'newColl')]).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1), new Document('c', 1)]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getWriteOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace, [new BsonDocument('$out', new BsonString('newColl'))]))
    }

    def 'should handle exceptions in aggregate correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Object>()

        when: 'The operation fails with an exception'
        collection.aggregate([new BsonDocument('$match', new BsonInt32(1))], BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then: 'the future should handle the exception'
        notThrown(MongoException)
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().size() == 1

        when: 'a codec is missing its acceptable to immediately throw'
        collection.aggregate([new Document('$match', 1)])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MapReduceWithInlineResultsOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([asyncDocCursor(), asyncDocCursor(), asyncDocCursor(), asyncDocCursor()])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                new DocumentCodec()).verbose(true)
        def bsonOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                new BsonDocumentCodec()).verbose(true)
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.mapReduce('map', 'reduce').subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1), new Document('c', 1)]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        def mapReduceOptions = new MapReduceOptions().finalizeFunction('final')
        collection.mapReduce('map', 'reduce', mapReduceOptions).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation.finalizeFunction(new BsonJavaScript('final')))

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.mapReduce('map', 'reduce', BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation.finalizeFunction(new BsonJavaScript('final')))
    }

    def 'should use MapReduceToCollectionOperation correctly'() {
        given:
        def stats = Stub(MapReduceStatistics)
        def executor = new TestOperationExecutor([stats, asyncDocCursor(), stats, asyncDocCursor()])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                'collectionName').filter(new BsonDocument('filter', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('final'))
                .verbose(true)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1)]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [new Document('a', 1), new Document('b', 1), new Document('c', 1)]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new DocumentCodec())
                .filter(new BsonDocument()))

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(3)

        then:
        subscriber.getReceived().size() == 3
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'),
                new BsonDocumentCodec())
                .filter(new BsonDocument()))
    }

    def 'should handle exceptions in mapReduce correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Document>()

        when: 'The operation fails with an exception'
        collection.mapReduce('map', 'reduce', BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then: 'the future should handle the exception'
        notThrown(MongoException)
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().size() == 1

        when: 'a codec is missing its acceptable to immediately throw'
        collection.mapReduce('map', 'reduce')

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                    ordered, writeConcern)
        }
        def subscriber = new ObservableSubscriber<BulkWriteResult>()

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))]).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        expect operation, isTheSameAs(expectedOperation(false))

        when:
        subscriber = new ObservableSubscriber<BulkWriteResult>()
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true)).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        expect operation, isTheSameAs(expectedOperation(true))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, 0, []),
                                                                 BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([BulkWriteResult.unacknowledged(),
                                                                 BulkWriteResult.unacknowledged()])
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use InsertOneOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new InsertOperation(namespace, true, writeConcern,
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])
        def subscriber = new ObservableSubscriber<Void>();

        when:
        collection.insertOne(new Document('_id', 1)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, false, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged()])

    }

    def 'should use InsertManyOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { ordered ->
            new InsertOperation(namespace, ordered, writeConcern,
                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                     new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])
        }
        def subscriber = new ObservableSubscriber<Void>()

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)]).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        subscriber = new ObservableSubscriber<Void>()
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true))
                .subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as InsertOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(true))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, false, null),
                                                                 WriteConcernResult.acknowledged(1, false, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use DeleteOneOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def subscriber = new ObservableSubscriber<DeleteResult>()

        when:
        collection.deleteOne(new Document('_id', 1)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(new DeleteOperation(namespace, true, writeConcern,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)]))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged()])
    }

    def 'should use DeleteManyOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def subscriber = new ObservableSubscriber<DeleteResult>()

        when:
        collection.deleteMany(new Document('_id', 1)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(new DeleteOperation(namespace, true, writeConcern,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))]))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged()])
    }

    @SuppressWarnings('LineLength')
    def 'replaceOne should use MixedBulkWriteOperationperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def subscriber = new ObservableSubscriber<UpdateResult>()

        when:
        collection.replaceOne(new Document('a', 1), new Document('a', 10)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                new BsonDocument('a', new BsonInt32(10)),
                REPLACE)],
                true, writeConcern))

        where:
        writeConcern                | executor                                                        | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, null, [])]) | UpdateResult.acknowledged(1, null, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, 1, [])])    | UpdateResult.acknowledged(1, 1, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, 1,
                [new BulkWriteUpsert(0, new BsonInt32(42))])])                                        | UpdateResult.acknowledged(1, 1, new BsonInt32(42))
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged()])                   | UpdateResult.unacknowledged()
    }

    def 'updateOne should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean upsert ->
            new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                    new BsonDocument('a', new BsonInt32(10)),
                    UPDATE).multi(false).upsert(upsert)],
                    true, writeConcern)
        }
        def subscriber = new ObservableSubscriber<UpdateResult>()

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        subscriber = new ObservableSubscriber<UpdateResult>()
        collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(true))

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(UPDATE, 1, []),
                                                                 acknowledged(UPDATE, 1, [])]) | UpdateResult.acknowledged(1, 0, null)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])            | UpdateResult.unacknowledged()
    }

    def 'updateMany should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean upsert ->
            new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                    new BsonDocument('a', new BsonInt32(10)),
                    UPDATE)
                                                            .multi(true).upsert(upsert)],
                    true, writeConcern)
        }
        def subscriber = new ObservableSubscriber<UpdateResult>()

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived() == expectedResult
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        subscriber = new ObservableSubscriber<UpdateResult>()
        collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        subscriber.getReceived() == expectedResult
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation(true))

        where:
        writeConcern                | executor                                                    | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(UPDATE, 5, 3, []),
                                                                 acknowledged(UPDATE, 5, 3, [])]) | [UpdateResult.acknowledged(5, 3, null)]
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])               | [UpdateResult.unacknowledged()]
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, new DocumentCodec()).filter(new BsonDocument('a', new BsonInt32(1)))
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.findOneAndDelete(new Document('a', 1)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1)))
                .subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                new FindOneAndReplaceOptions().projection(new Document('projection', 1))).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)

        when:
        subscriber = new ObservableSubscriber<Document>()
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                new FindOneAndUpdateOptions().projection(new Document('projection', 1))).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        subscriber.getReceived().head().wasAcknowledged() == writeConcern.isAcknowledged()
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropCollectionOperation(namespace)
        def subscriber = new ObservableSubscriber<Void>();

        when:
        collection.dropCollection().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CreateIndexOperation(namespace, new BsonDocument('key', new BsonInt32(1)))
        def subscriber = new ObservableSubscriber<Void>()

        when:
        collection.createIndex(new Document('key', 1)).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)

        when:
        subscriber = new ObservableSubscriber<Void>()
        collection.createIndex(new Document('key', 1), new CreateIndexOptions().background(true)).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation.background(true))
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def indexesDocs = [new Document('index', '1'), new Document('index', '2')]
        def indexesBson = [new BsonDocument('index', new BsonString('1')), new BsonDocument('index', new BsonString('2'))]
        def executor = new TestOperationExecutor([indexesDocs, indexesBson])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def subscriber = new ObservableSubscriber<Document>()

        when:
        collection.getIndexes().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        subscriber.getReceived() == [indexesDocs[0]]
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == indexesDocs
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        subscriber = new ObservableSubscriber<BsonDocument>()
        collection.getIndexes(BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(2)
        operation = executor.getReadOperation() as ListIndexesOperation

        then:
        subscriber.getReceived() == indexesBson
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new BsonDocumentCodec()))
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, 'indexName')
        def subscriber = new ObservableSubscriber<Void>()

        when:
        collection.dropIndex('indexName').subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*')
        def subscriber = new ObservableSubscriber<Void>()

        when:
        collection.dropIndexes().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use RenameCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace)
        def subscriber = new ObservableSubscriber<Void>()

        when:
        collection.renameCollection(newNamespace).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
        expect operation, isTheSameAs(expectedOperation)
    }

}
