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

import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class IterablePublisherVerification extends PublisherVerification<Integer> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 300L
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L

    private ExecutorService e

    @BeforeClass
    void before() { e = Executors.newFixedThreadPool(4) }

    @AfterClass
    void after() { e?.shutdown() }

    IterablePublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS)
    }

    @Override
    Publisher<String> createPublisher(long elements) {
        assert (elements <= maxElementsFromPublisher())
        new AsyncIterablePublisher<Integer>(new Iterable<Integer>() {
            @Override
            Iterator<Integer> iterator() {
                new Iterator<Integer>() {

                    private int at = 0

                    @Override
                    boolean hasNext() {
                        at < elements
                    }

                    @Override
                    Integer next() {
                        if (hasNext()) {
                            at++
                        } else {
                            Collections.<Integer> emptyList().iterator().next()
                        }
                    }

                    @Override
                    void remove() {
                        throw new UnsupportedOperationException('Removal not supported');
                    }
                }
            }
        }, e)
    }

    @Override
    Publisher<Integer> createErrorStatePublisher() {
        null
    }

    @Override
    long maxElementsFromPublisher() {
        Integer.MAX_VALUE
    }
}