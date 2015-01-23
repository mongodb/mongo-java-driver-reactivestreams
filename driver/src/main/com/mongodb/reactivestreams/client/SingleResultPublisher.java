/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.async.SingleResultCallback;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class SingleResultPublisher<T> implements Publisher<T> {

    @Override
    public void subscribe(final Subscriber<? super T> s) {
        new SingleResultSubscription(s).start();
    }

    SingleResultCallback<T> getCallback(final SubscriptionSupport<T> subscription) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable t) {
                subscription.log("result - " + result + " : " + t);
                if (t != null) {
                    subscription.onError(t);
                } else {
                    subscription.onNext(result);
                    subscription.onComplete();
                }
            }
        };
    }

    private class SingleResultSubscription extends SubscriptionSupport<T> {
        private final AtomicBoolean operationCompleted = new AtomicBoolean();

        public SingleResultSubscription(final Subscriber<? super T> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            log("doRequest : " + n);
            if (operationCompleted.compareAndSet(false, true)) {
                execute(getCallback(this));
            }
        }
    }

    abstract void execute(SingleResultCallback<T> callback);

    SingleResultPublisher() {
    }
}
