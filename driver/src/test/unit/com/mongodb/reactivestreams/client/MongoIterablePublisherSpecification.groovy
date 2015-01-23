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
        def batchedResults = [[new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)],
                              [new Document('_id', 4)] , [new Document('_id', 5)]]
        def cannedResults = batchedResults.flatten()
        def cursor = {
            Stub(AsyncBatchCursor) {
                next(_) >> {
                    it[0].onResult(batchedResults.remove(0), null)
                }
                isClosed() >> { batchedResults.isEmpty() }
            }
        }
        def subscriber = new Fixture.ObservableSubscriber()
        def mongoIterable = Stub(MongoIterable) {
            batchCursor(_) >> { args -> args[0].onResult(cursor(), null) }
        }

        when:
        new MongoIterablePublisher(mongoIterable).subscribe(subscriber)
        subscriber.getSubscription().request(1)  // 1 requested in total

        then:
        !subscriber.isCompleted()
        subscriber.getReceived() == [cannedResults[0]]

        when:
        subscriber.getSubscription().request(3) // 4 requested in total

        then:
        subscriber.getReceived() == cannedResults[0..3]
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(6) // 10 requested in total

        then:
        subscriber.getReceived() == cannedResults
        subscriber.isCompleted()
    }
}
