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
import com.mongodb.async.SingleResultCallback
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class SingleResultPublisherSpecification extends Specification {

    def 'should not execute until first request'() {
        given:
        def called = false;
        def subscriber = new Fixture.ObservableSubscriber()
        def publisher = new SingleResultPublisher() {
            @Override
            void execute(final SingleResultCallback callback) {
                called = true
                callback.onResult(null, null);
            }
        }

        when: 'does nothing until requested'
        publisher.subscribe(subscriber)

        then:
        !called
        !subscriber.isCompleted()

        when:
        subscriber.getSubscription().request(1)

        then:
        called
        subscriber.isCompleted()
    }

    def 'should return the callback result'() {
        given:
        def subscriber = new Fixture.ObservableSubscriber()
        def publisher = new SingleResultPublisher() {
            @Override
            void execute(final SingleResultCallback callback) {
                callback.onResult('callback result', null);
            }
        }

        when:
        publisher.subscribe(subscriber)
        def result = subscriber.get(1, TimeUnit.SECONDS)

        then:
        result == ['callback result']
        subscriber.isCompleted()
    }

    def 'should return handle and surface errors result'() {
        given:
        def subscriber = new Fixture.ObservableSubscriber()
        def publisher = new SingleResultPublisher() {
            @Override
            void execute(final SingleResultCallback callback) {
                callback.onResult(null, new MongoException('failure'));
            }
        }

        when:
        publisher.subscribe(subscriber)
        subscriber.get(1, TimeUnit.SECONDS)

        then:
        thrown(MongoException)
        subscriber.isCompleted()
    }
}
