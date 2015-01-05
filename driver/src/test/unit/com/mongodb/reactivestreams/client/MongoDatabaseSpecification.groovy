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

import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CommandWriteOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.ListCollectionsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static Fixture.ObservableSubscriber
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.ReadPreference.secondaryPreferred
import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def options = OperationOptions.builder().readPreference(secondary()).codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should return the correct options'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor([]))

        expect:
        database.getOptions() == options
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
        def database = new MongoDatabaseImpl(name, options, executor)
        def subscriber = new ObservableSubscriber<Document>();

        when:
        database.executeCommand(command).subscribe(subscriber)

        then:
        executor.getWriteOperation() == null
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        def operation = executor.getWriteOperation() as CommandWriteOperation<Document>
        operation.command == command
        subscriber.getReceived().size() == 1
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        subscriber = new ObservableSubscriber<Document>();
        database.executeCommand(command, primaryPreferred()).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()
        subscriber.getReceived().size() == 1
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        subscriber = new ObservableSubscriber<BsonDocument>();
        database.executeCommand(command, BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getWriteOperation() as CommandWriteOperation<BsonDocument>

        then:
        operation.command == command
        subscriber.getReceived().size() == 1
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        subscriber = new ObservableSubscriber<BsonDocument>();
        database.executeCommand(command, primaryPreferred(), BsonDocument).subscribe(subscriber)
        subscriber.getSubscription().request(1)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()
        subscriber.getReceived().size() == 1
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

    }

    def 'should use DropDatabaseOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def subscriber = new ObservableSubscriber<Void>();

        when:
        new MongoDatabaseImpl(name, options, executor).dropDatabase().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        subscriber.isCompleted()

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()

        when:
        def operation = executor.getWriteOperation() as DropDatabaseOperation

        then:
        expect operation, isTheSameAs(new DropDatabaseOperation(name))

        when:
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
    }

    def 'should use ListCollectionsOperation correctly'() {
        given:
        List<Document> names = (1..50).collect({ new Document('name', 'collection' + it) })
        def asyncCursor = Stub(AsyncBatchCursor) {
            def seen = false;
            next(_) >> { args ->
                seen = true
                args[0].onResult(names, null)
            }
            isClosed() >> { seen }
        }

        def executor = new TestOperationExecutor([asyncCursor])
        def subscriber = new ObservableSubscriber<String>();

        when:
        new MongoDatabaseImpl(name, options, executor).getCollectionNames().subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getReadOperation() == null

        when:
        subscriber.getSubscription().request(10)

        then:
        subscriber.getReceived().size() == 10
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        def operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation(name, new DocumentCodec()))
        executor.getReadPreference() == primary()

        when:
        subscriber.getSubscription().request(30)

        then:
        subscriber.getReceived().size() == 40
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(20)

        then:
        subscriber.getReceived() == names*.getString('name')
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, options, executor)
        def subscriber = new ObservableSubscriber<Void>();

        when:
        database.createCollection(collectionName).subscribe(subscriber)

        then:
        subscriber.getReceived().isEmpty()
        subscriber.getErrors().isEmpty()
        !subscriber.isCompleted()
        executor.getWriteOperation() == null

        when:
        subscriber.getSubscription().request(1)
        subscriber.isCompleted()

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()

        when:
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName))

        when:
        def createCollectionOptions = new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(new Document('wiredTiger', new Document()))

        subscriber = new ObservableSubscriber<Void>()
        database.createCollection(collectionName, createCollectionOptions).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.getReceived() == [null]
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()

        when:
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName)
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(new BsonDocument('wiredTiger', new BsonDocument())))
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def options = OperationOptions.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .codecRegistry(codecRegistry)
                .build()
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, options, executor)

        when:
        def collectionOptions = customOptions ? database.getCollection('name', customOptions).getOptions() :
                database.getCollection('name').getOptions()
        then:
        collectionOptions.getReadPreference() == readPreference
        collectionOptions.getWriteConcern() == writeConcern
        collectionOptions.getCodecRegistry() == codecRegistry

        where:
        customOptions                      | readPreference       | writeConcern              | codecRegistry
        null                               | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder().build() | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder()
                .readPreference(secondaryPreferred())
                .writeConcern(WriteConcern.MAJORITY)
                .build()                   | secondaryPreferred() | WriteConcern.MAJORITY     | new RootCodecRegistry([])

    }
}
