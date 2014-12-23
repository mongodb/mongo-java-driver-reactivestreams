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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncWriteOperation;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

class WriteOperationPublisher<T> implements Publisher<T> {
    private final AsyncWriteOperation<T> writeOperation;
    private final AsyncOperationExecutor executor;

    public WriteOperationPublisher(final AsyncWriteOperation<T> operation, final AsyncOperationExecutor executor) {
        this.writeOperation = operation;
        this.executor = executor;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        new WriteSubscription(subscriber).start();
    }

    private class WriteSubscription extends SubscriptionSupport<T> {
        private final AtomicBoolean operationCompleted = new AtomicBoolean();

        public WriteSubscription(final Subscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            log("doRequest : " + n);
            if (operationCompleted.compareAndSet(false, true)) {
                executor.execute(writeOperation, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        log("result - " + result + " : " + t);
                        if (t != null) {
                            onError(t);
                        } else {
                            onNext(result);
                            onComplete();
                        }
                    }
                });
            }
        }
    }
}
