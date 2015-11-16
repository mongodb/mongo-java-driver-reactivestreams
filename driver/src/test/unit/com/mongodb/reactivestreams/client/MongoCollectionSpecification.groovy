/*
 * Copyright 2014-2015 MongoDB, Inc.
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
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.async.client.MongoCollection as WrappedMongoCollection
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.UpdateOptions
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoCollectionSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(1) }
    }
    def wrapped = Mock(WrappedMongoCollection)
    def mongoCollection = new MongoCollectionImpl(wrapped)
    def filter = new Document('_id', 1)

    def 'should have the same methods as the wrapped MongoCollection'() {
        given:
        def wrapped = WrappedMongoCollection.methods*.name.sort()
        def local = MongoCollection.methods*.name.sort()

        expect:
        wrapped == local
    }

    def 'should use the underlying getNamespace'() {
        when:
        mongoCollection.getNamespace()

        then:
        1 * wrapped.getNamespace()
    }

    def 'should use the underlying getDocumentClass'() {
        when:
        mongoCollection.getDocumentClass()

        then:
        1 * wrapped.getDocumentClass()
    }

    def 'should call the underlying getCodecRegistry'() {
        when:
        mongoCollection.getCodecRegistry()

        then:
        1 * wrapped.getCodecRegistry()
    }

    def 'should call the underlying getReadPreference'() {
        when:
        mongoCollection.getReadPreference()

        then:
        1 * wrapped.getReadPreference()

    }

    def 'should call the underlying getWriteConcern'() {
        when:
        mongoCollection.getWriteConcern()

        then:
        1 * wrapped.getWriteConcern()
    }

    def 'should call the underlying getReadConcern'() {
        when:
        mongoCollection.getReadConcern()

        then:
        1 * wrapped.getReadConcern()
    }

    def 'should use the underlying withDocumentClass'() {
        given:
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withDocumentClass(BsonDocument) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withDocumentClass(BsonDocument)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withCodecRegistry'() {
        given:
        def codecRegistry = Stub(CodecRegistry)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withCodecRegistry(codecRegistry) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withCodecRegistry(codecRegistry)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withReadPreference'() {
        given:
        def readPreference = Stub(ReadPreference)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withReadPreference(readPreference) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withReadPreference(readPreference)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withWriteConcern'() {
        given:
        def writeConcern = Stub(WriteConcern)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withWriteConcern(writeConcern) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withWriteConcern(writeConcern)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withReadConcern'() {
        given:
        def readConcern = ReadConcern.MAJORITY
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withReadConcern(readConcern) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withReadConcern(readConcern)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should use the underlying count'() {
        given:
        def options = new CountOptions()

        when:
        mongoCollection.count()

        then: 'only executed when requested'
        0 * wrapped.count(_, _, _)

        when:
        mongoCollection.count().subscribe(subscriber)

        then:
        1 * wrapped.count(_, _, _)

        when:
        mongoCollection.count(filter).subscribe(subscriber)

        then:
        1 * wrapped.count(filter, _, _)

        when:
        mongoCollection.count(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.count(filter, options, _)
    }


    def 'should create DistinctPublisher correctly'() {
        given:
        def wrapped = Stub(WrappedMongoCollection) {
            distinct(_, _) >> Stub(com.mongodb.async.client.DistinctIterable)
        }
        def collection = new MongoCollectionImpl(wrapped)

        when:
        def distinctPublisher = collection.distinct('field', String)

        then:
        expect distinctPublisher, isTheSameAs(new DistinctPublisherImpl(wrapped.distinct('field', String)))
    }

    def 'should create FindPublisher correctly'() {
        given:
        def wrappedResult = Stub(com.mongodb.async.client.FindIterable)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * find(new BsonDocument(), Document) >> wrappedResult
            1 * find(new BsonDocument(), BsonDocument) >> wrappedResult
            1 * find(new Document(), Document) >> wrappedResult
            1 * find(new Document(), BsonDocument) >> wrappedResult
            2 * getDocumentClass() >> Document
        }
        def collection = new MongoCollectionImpl(wrapped)

        when:
        def findPublisher = collection.find()

        then:
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(BsonDocument)

        then:
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(new Document())

        then:
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(new Document(), BsonDocument)

        then:
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))
    }

    def 'should use AggregatePublisher correctly'() {
        given:
        def wrappedResult = Stub(com.mongodb.async.client.AggregateIterable)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * aggregate(_, Document) >> wrappedResult
            1 * aggregate(_, BsonDocument) >> wrappedResult
        }
        def collection = new MongoCollectionImpl(wrapped)
        def pipeline = [new Document('$match', 1)]

        when:
        def aggregatePublisher = collection.aggregate(pipeline)

        then:
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = collection.aggregate(pipeline, BsonDocument)

        then:
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))
    }

    def 'should create MapReducePublisher correctly'() {
        given:
        def wrappedResult = Stub(com.mongodb.async.client.MapReduceIterable)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * mapReduce('map', 'reduce', Document) >> wrappedResult
            1 * mapReduce('map', 'reduce', BsonDocument) >> wrappedResult
        }
        def collection = new MongoCollectionImpl(wrapped)

        when:
        def mapReducePublisher = collection.mapReduce('map', 'reduce')

        then:
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))

        when:
        mapReducePublisher = collection.mapReduce('map', 'reduce', BsonDocument)

        then:
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))
    }


    def 'should use the underlying bulkWrite'() {
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def bulkOperation = [new InsertOneModel<Document>(new Document('_id', 10))]
        def options = new BulkWriteOptions()

        when:
        mongoCollection.bulkWrite(bulkOperation)

        then: 'only executed when requested'
        0 * wrapped.bulkWrite(_, _, _)

        when:
        mongoCollection.bulkWrite(bulkOperation).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(bulkOperation, _, _)

        when:
        mongoCollection.bulkWrite(bulkOperation, options).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(bulkOperation, options, _)
    }

    def 'should use the underlying insertOne'() {
        given:
        def insert = new Document('_id', 1)
        def options = new InsertOneOptions()

        when:
        mongoCollection.insertOne(insert)

        then: 'only executed when requested'
        0 * wrapped.insertOne(_)

        when:
        mongoCollection.insertOne(insert).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(insert, _, _)

        when:
        mongoCollection.insertOne(insert, options).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(insert, options, _)
    }

    def 'should use the underlying insertMany'() {
        given:
        def inserts = [new Document('_id', 1)]
        def options = new InsertManyOptions()

        when:
        mongoCollection.insertMany(inserts)

        then: 'only executed when requested'
        0 * wrapped.insertMany(_, _, _)

        when:
        mongoCollection.insertMany(inserts).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(inserts, _, _)

        when:
        mongoCollection.insertMany(inserts, options).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(inserts, options, _)
    }

    def 'should use the underlying deleteOne'() {
        when:
        mongoCollection.deleteOne(filter)

        then: 'only executed when requested'
        0 * wrapped.deleteOne(_, _)

        when:
        mongoCollection.deleteOne(filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteOne(filter, _)
    }

    def 'should use the underlying deleteMany'() {
        when:
        mongoCollection.deleteMany(filter)

        then: 'only executed when requested'
        0 * wrapped.deleteMany(_, _)

        when:
        mongoCollection.deleteMany(filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteMany(filter, _)
    }

    def 'should use the underlying replaceOne'() {
        given:
        def replacement = new Document('new', 1)
        def options = new UpdateOptions()

        when:
        mongoCollection.replaceOne(filter, replacement)

        then: 'only executed when requested'
        0 * wrapped.replaceOne(_, _, _, _)

        when:
        mongoCollection.replaceOne(filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(filter, replacement, _, _)

        when:
        mongoCollection.replaceOne(filter, replacement, options).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(filter, replacement, options, _)
    }


    def 'should use the underlying updateOne'() {
        given:
        def update = new Document('new', 1)
        def options = new UpdateOptions()

        when:
        mongoCollection.updateOne(filter, update)

        then: 'only executed when requested'
        0 * wrapped.updateOne(_, _, _, _)

        when:
        mongoCollection.updateOne(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(filter, update, _, _)

        when:
        mongoCollection.updateOne(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(filter, update, options, _)
    }

    def 'should use the underlying updateMany'() {
        given:
        def update = new Document('new', 1)
        def options = new UpdateOptions()

        when:
        mongoCollection.updateMany(filter, update)

        then: 'only executed when requested'
        0 * wrapped.updateMany(_, _, _, _)

        when:
        mongoCollection.updateMany(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(filter, update, _, _)

        when:
        mongoCollection.updateMany(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(filter, update, options, _)
    }

    def 'should use the underlying findOneAndDelete'() {
        given:
        def options = new FindOneAndDeleteOptions()

        when:
        mongoCollection.findOneAndDelete(filter)

        then: 'only executed when requested'
        0 * wrapped.findOneAndDelete(_, _, _)

        when:
        mongoCollection.findOneAndDelete(filter).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(filter, _, _)

        when:
        mongoCollection.findOneAndDelete(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(filter, options, _)

    }

    def 'should use the underlying findOneAndReplace'() {
        given:
        def replacement = new Document('new', 1)
        def options = new FindOneAndReplaceOptions()

        when:
        mongoCollection.findOneAndReplace(filter, replacement)

        then: 'only executed when requested'
        0 * wrapped.findOneAndReplace(_, _, _, _)

        when:
        mongoCollection.findOneAndReplace(filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(filter, replacement, _, _)

        when:
        mongoCollection.findOneAndReplace(filter, replacement, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(filter, replacement, options, _)
    }

    def 'should use the underlying findOneAndUpdate'() {
        given:
        def update = new Document('new', 1)
        def options = new FindOneAndUpdateOptions()

        when:
        mongoCollection.findOneAndUpdate(filter, update)

        then: 'only executed when requested'
        0 * wrapped.findOneAndUpdate(_, _, _, _)

        when:
        mongoCollection.findOneAndUpdate(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(filter, update, _, _)

        when:
        mongoCollection.findOneAndUpdate(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(filter, update, options, _)
    }

    def 'should use the underlying drop'() {
        when:
        mongoCollection.drop()

        then: 'only executed when requested'
        0 * wrapped.drop(_)

        when:
        mongoCollection.drop().subscribe(subscriber)

        then:
        1 * wrapped.drop(_)
    }

    def 'should use the underlying createIndex'() {
        given:
        def index = new Document('index', 1)
        def options = new IndexOptions()

        when:
        mongoCollection.createIndex(index)

        then: 'only executed when requested'
        0 * wrapped.createIndex(_, _, _)

        when:
        mongoCollection.createIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(index, _, _)

        when:
        mongoCollection.createIndex(index, options).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(index, options, _)
    }

    def 'should use the underlying createIndexes'() {
        given:
        def indexes = [new IndexModel(new Document('index', 1))]

        when:
        mongoCollection.createIndexes(indexes)

        then: 'only executed when requested'
        0 * wrapped.createIndexes(_, _)

        when:
        mongoCollection.createIndexes(indexes).subscribe(subscriber)

        then:
        1 * wrapped.createIndexes(indexes, _)
    }

    def 'should use the underlying listIndexes'() {
        def wrapped = Stub(WrappedMongoCollection) {
            listIndexes(_) >> Stub(com.mongodb.async.client.ListIndexesIterable)
            getDocumentClass() >> Document
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def publisher = mongoCollection.listIndexes()

        then:
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(wrapped.listIndexes(Document)))

        when:
        mongoCollection.listIndexes(BsonDocument)

        then:
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(wrapped.listIndexes(BsonDocument)))
    }

    def 'should use the underlying dropIndex'() {
        given:
        def index = 'index'

        when:
        mongoCollection.dropIndex(index)

        then: 'only executed when requested'
        0 * wrapped.dropIndex(_, _)

        when:
        mongoCollection.dropIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(index, _)

        when:
        index = new Document('index', 1)
        mongoCollection.dropIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(_, _)

        when:
        mongoCollection.dropIndexes().subscribe(subscriber)

        then:
        1 * wrapped.dropIndex('*', _)
    }

    def 'should use the underlying renameCollection'() {
        given:
        def nameCollectionNamespace = new MongoNamespace('db', 'coll')
        def options = new RenameCollectionOptions()

        when:
        mongoCollection.renameCollection(nameCollectionNamespace)

        then: 'only executed when requested'
        0 * wrapped.renameCollection(_, _, _)

        when:
        mongoCollection.renameCollection(nameCollectionNamespace).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(nameCollectionNamespace, _, _)

        when:
        mongoCollection.renameCollection(nameCollectionNamespace, options).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(nameCollectionNamespace, options, _)
    }

}
