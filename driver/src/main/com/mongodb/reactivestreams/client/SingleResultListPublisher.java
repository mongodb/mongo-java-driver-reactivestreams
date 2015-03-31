package com.mongodb.reactivestreams.client;

import com.mongodb.async.SingleResultCallback;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class SingleResultListPublisher<TResult> implements Publisher<TResult> {

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new SingleResultSubscription(s).start();
    }

    SingleResultCallback<List<TResult>> getCallback(final SubscriptionSupport<TResult> subscription) {
        return new SingleResultCallback<List<TResult>>() {
            @Override
            public void onResult(final List<TResult> results, final Throwable t) {
                subscription.log("result - " + results + " : " + t);
                if (t != null) {
                    subscription.onError(t);
                } else {
                    if (results != null) {
                        for (TResult result : results) {
                            subscription.onNext(result);
                        }
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

    abstract void execute(SingleResultCallback<List<TResult>> callback);

    SingleResultListPublisher() {
    }
}
