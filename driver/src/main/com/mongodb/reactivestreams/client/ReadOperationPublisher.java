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

package com.mongodb.reactivestreams.client;

import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

class ReadOperationPublisher<T> implements MongoPublisher<T> {

    private final AsyncReadOperation<T> readOperation;
    private final ReadPreference readPreference;
    private final AsyncOperationExecutor executor;

    public ReadOperationPublisher(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                                  final AsyncOperationExecutor executor) {
        this.readOperation = operation;
        this.readPreference = readPreference;
        this.executor = executor;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        new Subscription(subscriber).start();
    }

    @Override
    public <O> MongoPublisher<O> map(final Function<? super T, ? extends O> function) {
        return Publishers.map(this, function);
    }

    private class Subscription extends SubscriptionSupport<T> {
        private final AtomicBoolean operationCompleted = new AtomicBoolean(false);

        public Subscription(final Subscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            log("doRequest : " + n);
            if (operationCompleted.compareAndSet(false, true)) {
                executor.execute(readOperation, readPreference, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        log("result - " + result + " : " + t);
                        if (t != null) {
                            onError(t);
                        } else {
                            onNext(result);
                        }
                        onComplete();
                    }
                });
            }
        }
    }
}
