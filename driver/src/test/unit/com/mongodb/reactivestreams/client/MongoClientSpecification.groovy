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
import com.mongodb.client.options.OperationOptions
import com.mongodb.connection.Cluster
import com.mongodb.operation.GetDatabaseNamesOperation
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.ReadPreference.secondaryPreferred
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should use GetDatabaseNamesOperation correctly'() {
        given:
        List<String> names = (1..50).collect({ 'collection' + it })
        def options = MongoClientOptions.builder().build()
        def cluster = Stub(Cluster)
        def executor = new TestOperationExecutor([(1..50).collect({ 'collection' + it })])
        def subscriber = new Fixture.ObservableSubscriber<String>();

        when:
        new MongoClientImpl(options, cluster, executor).getDatabaseNames().subscribe(subscriber)

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
        def operation = executor.getReadOperation() as GetDatabaseNamesOperation

        then:
        expect operation, isTheSameAs(new GetDatabaseNamesOperation())
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
        subscriber.getReceived() == names
        subscriber.getErrors().isEmpty()
        subscriber.isCompleted()
    }

    def 'should provide the same options'() {
        given:
        def options = MongoClientOptions.builder().build()

        when:
        def clientOptions = new MongoClientImpl(options, Stub(Cluster), new TestOperationExecutor([])).getOptions()

        then:
        options == clientOptions
    }

    def 'should pass the correct options to getDatabase'() {
        given:
        def options = MongoClientOptions.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .codecRegistry(codecRegistry)
                .build()
        def client = new MongoClientImpl(options, Stub(Cluster), new TestOperationExecutor([]))

        when:
        def databaseOptions = customOptions ? client.getDatabase('name', customOptions).getOptions() :
                client.getDatabase('name').getOptions()
        then:
        databaseOptions.getReadPreference() == readPreference
        databaseOptions.getWriteConcern() == writeConcern
        databaseOptions.getCodecRegistry() == codecRegistry

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
