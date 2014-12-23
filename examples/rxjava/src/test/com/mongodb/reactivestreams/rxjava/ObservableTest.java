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

package com.mongodb.reactivestreams.rxjava;

import com.mongodb.reactivestreams.client.MongoDatabase;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static rx.RxReactiveStreams.toObservable;

public class ObservableTest {

    @Test
    public void testInteropWithRxJava() {

        MongoDatabase database = Fixture.getDefaultDatabase();
        
        toObservable(database.dropDatabase()).timeout(10, SECONDS).toBlocking().single();

        List<Observable<Void>> observers = new ArrayList<Observable<Void>>();
        List<String> uppercaseNames = new ArrayList<String>();
        
        for (int i = 0; i < 25; i++) {
            String name = "collectionNumber" + i;
            observers.add(toObservable(database.createCollection(name)));
            uppercaseNames.add(name.toUpperCase());
        }
        assertThat(Observable.merge(observers).timeout(10, SECONDS).toList().toBlocking().single().size(), is(25));

        Observable<String> collectionNames = toObservable(database.getCollectionNames())
                .filter(new Func1<String, Boolean>() {
                    @Override
                    public Boolean call(final String s) {
                        return s.startsWith("c");
                    }
                }).map(new Func1<String, String>() {
                    @Override
                    public String call(final String s) {
                        return s.toUpperCase();
                    }
                });

        assertThat(collectionNames.toList().timeout(10, SECONDS).toBlocking().single(), containsInAnyOrder(uppercaseNames.toArray()));
    }


}