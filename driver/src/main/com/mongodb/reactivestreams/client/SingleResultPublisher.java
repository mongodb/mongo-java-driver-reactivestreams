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

abstract class SingleResultPublisher<TResult> implements Publisher<TResult> {

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new SingleResultSubscription(s).start();
    }

    SingleResultCallback<TResult> getCallback(final SubscriptionSupport<TResult> subscription) {
        return new SingleResultCallback<TResult>() {
            @Override
            public void onResult(final TResult result, final Throwable t) {
                subscription.log("result - " + result + " : " + t);
                if (t != null) {
                    subscription.onError(t);
                } else {
                    if (result != null) {
                        subscription.onNext(result);
                    }
                    subscription.onComplete();
                }
            }
        };
    }

    private class SingleResultSubscription extends SubscriptionSupport<TResult> {
        private final AtomicBoolean operationCompleted = new AtomicBoolean();

        public SingleResultSubscription(final Subscriber<? super TResult> subscriber) {
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

    abstract void execute(SingleResultCallback<TResult> callback);

    SingleResultPublisher() {
    }
}
