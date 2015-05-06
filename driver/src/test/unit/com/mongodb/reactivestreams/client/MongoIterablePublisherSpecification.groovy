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

import com.mongodb.MongoException
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.client.MongoIterable
import org.bson.Document
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class MongoIterablePublisherSpecification extends Specification {

    def 'should be cold and not execute the batchCursor until request is called'() {
        given:
        def subscriber = new Fixture.ObservableSubscriber()
        def mongoIterable = Mock(MongoIterable)

        when:
        def publisher = new MongoIterablePublisher(mongoIterable)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        publisher.subscribe(subscriber)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        subscriber.getSubscription().request(1)

        then:
        1 * mongoIterable.batchSize(2) // minimum batchSize is 2
        1 * mongoIterable.batchCursor(_)

        when: 'Is an integer greater than 1 should just set the number'
        subscriber = new Fixture.ObservableSubscriber()
        publisher.subscribe(subscriber)
        subscriber.getSubscription().request(100)

        then:
        1 * mongoIterable.batchSize(100)
        1 * mongoIterable.batchCursor(_)

        when: 'Amounts greater than 1024 are batched'
        subscriber = new Fixture.ObservableSubscriber()
        publisher.subscribe(subscriber)
        subscriber.getSubscription().request(10000)

        then:
        1 * mongoIterable.batchSize(1024)
        1 * mongoIterable.batchCursor(_)
    }

    def 'should handle exceptions when getting the batchCursor'() {
        given:
        def subscriber = new Fixture.ObservableSubscriber()
        def mongoIterable = Stub(MongoIterable) {
            batchCursor(_) >> { args -> args[0].onResult(null, new MongoException('failure')) }
        }

        when:
        new MongoIterablePublisher(mongoIterable).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.isCompleted()

        when:
        subscriber.get(1, TimeUnit.SECONDS)

        then:
        thrown(MongoException)
    }

    def 'should handle null returns when getting the batchCursor'() {
        given:
        def subscriber = new Fixture.ObservableSubscriber()
        def mongoIterable = Stub(MongoIterable) {
            batchCursor(_) >> { args -> args[0].onResult(null, null) }
        }

        when:
        new MongoIterablePublisher(mongoIterable).subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        subscriber.isCompleted()

        when:
        subscriber.get(1, TimeUnit.SECONDS)

        then:
        thrown(MongoException)
    }

    def 'should call next on the batchCursor'() {
        given:
        def batchSize = 10
        def batches = 10
        def cannedResults = (1..(batchSize * batches)).collect({ new Document('_id', it) })
        def cursorResults = cannedResults.collate(batchSize)
        def cursor = {
            Stub(AsyncBatchCursor) {
                next(_) >> {
                    it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
                }
            }
        }
        def subscriber = new Fixture.ObservableSubscriber()
        def mongoIterable = Stub(MongoIterable) {
            batchCursor(_) >> { args -> args[0].onResult(cursor(), null) }
        }

        when:
        new MongoIterablePublisher(mongoIterable).subscribe(subscriber)
        subscriber.getSubscription().request(10)  // First batch

        then:
        !subscriber.isCompleted()
        subscriber.getReceived() == cannedResults[0..9]

        when:
        subscriber.getSubscription().request(15) // Request across batches

        then:
        subscriber.getReceived() == cannedResults[0..24]
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(55) // Request across batches

        then:
        subscriber.getReceived() == cannedResults[0..79]
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(99)  // Request more than is left

        then:
        subscriber.getReceived() == cannedResults
        subscriber.isCompleted()
    }
}
