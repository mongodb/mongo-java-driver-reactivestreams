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

import com.mongodb.CursorType
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.client.FindIterable
import com.mongodb.async.client.FindIterableImpl
import com.mongodb.async.client.MongoIterable
import com.mongodb.client.model.FindOptions
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class FindPublisherSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(100) }
    }
    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new DocumentCodecProvider(), new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'should have the same methods as the wrapped FindIterable'() {
        given:
        def wrapped = (FindIterable.methods*.name - MongoIterable.methods*.name).sort()
        def local = (FindPublisher.methods*.name - Publisher.methods*.name - 'first' - 'batchSize').sort()

        expect:
        wrapped == local
    }

    def 'should build the expected findOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def findOptions = new FindOptions().sort(new Document('sort', 1))
                .modifiers(new Document('modifier', 1))
                .projection(new Document('projection', 1))
                .maxTime(1000, MILLISECONDS)
                .maxAwaitTime(2000, MILLISECONDS)
                .batchSize(1) // Overriden by the subscriber
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .oplogReplay(false)
                .noCursorTimeout(false)
                .partial(false)
        def wrapped = new FindIterableImpl<Document, Document>(namespace, Document, Document, codecRegistry, secondary(),
                ReadConcern.DEFAULT, executor, new Document('filter', 1), findOptions)
        def findPublisher = new FindPublisherImpl<Document>(wrapped)

        when: 'default input should be as expected'
        findPublisher.subscribe(subscriber)  // sets the batchSize

        def operation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(1, SECONDS)
                .maxAwaitTime(2, SECONDS)
                .batchSize(100)
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
        )
        readPreference == secondary()

        when: 'overriding initial options'
        findPublisher.filter(new Document('filter', 2))
                .sort(new Document('sort', 2))
                .modifiers(new Document('modifier', 2))
                .projection(new Document('projection', 2))
                .maxTime(999, MILLISECONDS)
                .limit(99)
                .skip(9)
                .cursorType(CursorType.Tailable)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .subscribe(subscriber)

        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2)))
                .sort(new BsonDocument('sort', new BsonInt32(2)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(2)))
                .projection(new BsonDocument('projection', new BsonInt32(2)))
                .maxTime(999, MILLISECONDS)
                .batchSize(100)
                .limit(99)
                .skip(9)
                .cursorType(CursorType.Tailable)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .slaveOk(true)
        )
    }

    def 'should build the expected findOperation for first'() {
        given:
        def cannedResults = [new Document('_id', 1)]
        def cursor = {
            Stub(AsyncBatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    if (count < 1) {
                        results = cannedResults
                    } else {
                        results = null
                    }
                    count++
                    results
                }
                next(_) >> {
                    it[0].onResult(getResult(), null)
                }
                isClosed() >> { count >= 1 }
            }
        }
        def executor = new TestOperationExecutor([cursor()]);
        def wrapped = new FindIterableImpl<Document, Document>(namespace, Document, Document, codecRegistry, primary(),
                ReadConcern.DEFAULT, executor, new Document(), new FindOptions())
        def findPublisher = new FindPublisherImpl<Document>(wrapped)

        when: 'default input should be as expected'
        findPublisher.first().subscribe(subscriber)

        def operation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument())
                .batchSize(0)
                .limit(-1)
        )
        readPreference == primary()
    }

    def 'should handle mixed types'() {
        given:

        def executor = new TestOperationExecutor([null]);
        def findOptions = new FindOptions()
        def wrapped = new FindIterableImpl<Document, Document>(namespace, Document, Document, codecRegistry, secondary(),
                ReadConcern.DEFAULT, executor, new Document('filter', 1), findOptions)
        def findPublisher = new FindPublisherImpl<Document>(wrapped)

        when:
        findPublisher.filter(new Document('filter', 1))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new Document('modifier', 1))
                .subscribe(subscriber)

        def operation = executor.getReadOperation() as FindOperation<Document>

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
                .batchSize(100)
        )
    }

}
