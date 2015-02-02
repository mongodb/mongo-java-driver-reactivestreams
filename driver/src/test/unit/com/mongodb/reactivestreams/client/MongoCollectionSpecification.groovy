/*
 * Copyright 2014 MongoDB, Inc.
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
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.async.client.MongoCollection as WrappedMongoCollection
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
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.UpdateOptions
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
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

    def 'should use the underlying getDefaultClass'() {
        when:
        mongoCollection.getDefaultClass()

        then:
        1 * wrapped.getDefaultClass()
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

    def 'should use the underlying withDefaultClass'() {
        given:
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withDefaultClass(BsonDocument) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withDefaultClass(BsonDocument)

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

    def 'should use the underlying distinct'() {
        given:
        def fieldName = 'fieldName'
        def options = new DistinctOptions()
        def wrapped = Stub(WrappedMongoCollection) {
            distinct(_, _, _, _) >> Stub(com.mongodb.async.client.MongoIterable)
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def iterable = mongoCollection.distinct(fieldName, filter, String)

        then:
        expect iterable, isTheSameAs(new MongoIterablePublisher(wrapped.distinct(fieldName, filter, options, String)))

        when:
        mongoCollection.distinct(fieldName, filter, options, String).subscribe(subscriber)

        then:
        expect iterable, isTheSameAs(new MongoIterablePublisher(wrapped.distinct(fieldName, filter, options, String)))
    }

    def 'should use the underlying find'() {
        given:
        def wrapped = Stub(WrappedMongoCollection) {
            find(_, _) >> Stub(com.mongodb.async.client.FindFluent)
            getDefaultClass() >> Document
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def fluent = mongoCollection.find()

        then:
        expect fluent, isTheSameAs(new FindFluentImpl(wrapped.find(new Document(), Document)))

        when:
        fluent = mongoCollection.find(BsonDocument)

        then:
        expect fluent, isTheSameAs(new FindFluentImpl(wrapped.find(new Document(), BsonDocument)))

        when:
        fluent = mongoCollection.find(filter)

        then:
        expect fluent, isTheSameAs(new FindFluentImpl(wrapped.find(filter, Document)))

        when:
        fluent = mongoCollection.find(filter, BsonDocument)

        then:
        expect fluent, isTheSameAs(new FindFluentImpl(wrapped.find(filter, BsonDocument)))
    }

    def 'should use the underlying aggregate'() {
        def wrapped = Stub(WrappedMongoCollection) {
            aggregate(_, _, _) >> Stub(com.mongodb.async.client.MongoIterable)
            getDefaultClass() >> Document
        }
        def pipeline = [new Document('$match', new Document('_id', 1))]
        def options = new AggregateOptions()
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def fluent = mongoCollection.aggregate(pipeline)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.aggregate(pipeline, options, Document)))

        when:
        mongoCollection.aggregate(pipeline, options)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.aggregate(pipeline, options, Document)))

        when:
        mongoCollection.aggregate(pipeline, options, BsonDocument)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.aggregate(pipeline, options, BsonDocument)))
    }

    def 'should use the underlying aggregateToCollection'() {
        given:
        def pipeline = [new Document('$match', new Document('_id', 1))]
        def options = new AggregateOptions()

        when:
        mongoCollection.aggregateToCollection(pipeline)

        then: 'only executed when requested'
        0 * wrapped.aggregateToCollection(_, _, _)

        when:
        mongoCollection.aggregateToCollection(pipeline).subscribe(subscriber)

        then:
        1 * wrapped.aggregateToCollection(pipeline, _, _)

        when:
        mongoCollection.aggregateToCollection(pipeline, options).subscribe(subscriber)

        then:
        1 * wrapped.aggregateToCollection(pipeline, options, _)
    }

    def 'should use the underlying mapReduce'() {
        def wrapped = Stub(WrappedMongoCollection) {
            mapReduce(_, _, _, _) >> Stub(com.mongodb.async.client.MongoIterable)
            getDefaultClass() >> Document
        }
        def mapFunction = 'map'
        def reduceFunction = 'reduce'
        def options = new MapReduceOptions()
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def fluent = mongoCollection.mapReduce(mapFunction, reduceFunction)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.mapReduce(mapFunction, reduceFunction, options, Document)))

        when:
        fluent = mongoCollection.mapReduce(mapFunction, reduceFunction, options)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.mapReduce(mapFunction, reduceFunction, options, Document)))


        when:
        fluent = mongoCollection.mapReduce(mapFunction, reduceFunction, Document)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.mapReduce(mapFunction, reduceFunction, options, Document)))

        when:
        fluent = mongoCollection.mapReduce(mapFunction, reduceFunction, options, BsonDocument)

        then:
        expect fluent, isTheSameAs(new MongoIterablePublisher(wrapped.mapReduce(mapFunction, reduceFunction, options, BsonDocument)))
    }

    def 'should use the underlying mapReduceToCollection'() {
        given:
        def mapFunction = 'map'
        def reduceFunction = 'reduce'
        def options = new MapReduceOptions()

        when:
        mongoCollection.mapReduceToCollection(mapFunction, reduceFunction, options)

        then: 'only executed when requested'
        0 * wrapped.mapReduceToCollection(_, _, _, _)

        when:
        mongoCollection.mapReduceToCollection(mapFunction, reduceFunction, options).subscribe(subscriber)

        then:
        1 * wrapped.mapReduceToCollection(mapFunction, reduceFunction, options, _)
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

        when:
        mongoCollection.insertOne(insert)

        then: 'only executed when requested'
        0 * wrapped.insertOne(_)

        when:
        mongoCollection.insertOne(insert).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(insert, _)
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

    def 'should use the underlying dropCollection'() {
        when:
        mongoCollection.dropCollection()

        then: 'only executed when requested'
        0 * wrapped.dropCollection(_)

        when:
        mongoCollection.dropCollection().subscribe(subscriber)

        then:
        1 * wrapped.dropCollection(_)
    }

    def 'should use the underlying createIndex'() {
        given:
        def index = new Document('index', 1)
        def options = new CreateIndexOptions()

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

    def 'should use the underlying listIndexes'() {
        def wrapped = Stub(WrappedMongoCollection) {
            listIndexes(_) >> Stub(com.mongodb.async.client.ListIndexesFluent)
            getDefaultClass() >> Document
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def fluent = mongoCollection.listIndexes()

        then:
        expect fluent, isTheSameAs(new ListIndexesFluentImpl(wrapped.listIndexes(Document)))

        when:
        mongoCollection.listIndexes(BsonDocument)

        then:
        expect fluent, isTheSameAs(new ListIndexesFluentImpl(wrapped.listIndexes(BsonDocument)))
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
