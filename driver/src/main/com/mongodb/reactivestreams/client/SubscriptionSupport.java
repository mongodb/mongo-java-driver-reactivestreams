/*
 * Copyright 2014 the original author or authors.
 * Copyright 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Based upon an implementation of a Subscriber in the AsyncIterablePublisher example http://www.reactive-streams.org/
 *
 * @param <T> The publisher result type.
 */
abstract class SubscriptionSupport<T> implements Subscription {

    private static final Logger LOGGER = Loggers.getLogger("reactivestreams");
    private static final int DEFAULT_BATCHSIZE = 1024;

    private final int batchSize;  // The max batchSize to use
    private final Subscriber<? super T> subscriber; // We need a reference to the `Subscriber` so we can talk to it
    private volatile boolean started = false;   // Tracks whether this `Subscription` is to have started
    private boolean completed = false; // Tracks whether this `Subscription` is to be considered completed or not
    private boolean cancelled = false; // Tracks whether this `Subscription` is to be considered cancelled or not
    private long demand = 0; // Here we track the current demand, i.e. what has been requested but not yet delivered

    // This `ConcurrentLinkedQueue` will track signals that are sent to this `Subscription`, like `request` and `cancel`
    private final ConcurrentLinkedQueue<Signal> inboundSignals = new ConcurrentLinkedQueue<Signal>();
    private final ConcurrentLinkedQueue<Result<T>> resultsQueue = new ConcurrentLinkedQueue<Result<T>>();

    // We are using this `AtomicBoolean` to make sure that this `Subscription` doesn't run concurrently with itself,
    // which would violate rule 1.3 among others (no concurrent notifications).
    private final AtomicBoolean on = new AtomicBoolean(false);

    public SubscriptionSupport(final Subscriber<? super T> subscriber) {
        this(subscriber, DEFAULT_BATCHSIZE);
    }

    SubscriptionSupport(final Subscriber<? super T> subscriber, final int batchSize) {
        isTrueArgument("batchSize must be greater than zero!", batchSize > 0);
        this.subscriber = notNull("subscriber", subscriber);
        this.batchSize = batchSize;
        log("constructor");
    }

    // This method will register inbound demand from our `Subscriber` and validate it against rule 3.9 and rule 3.17
    private void handleRequest(final long n) {
        if (n < 1) {
            terminateDueTo(new IllegalArgumentException(subscriber + " violated the Reactive Streams rule 3.9 by "
                    + "requesting a non-positive number of elements."));
        } else if (demand + n < 1) {
            terminateDueTo(new IllegalStateException(subscriber + " violated the Reactive Streams rule 3.17 by "
                    + "demanding more elements than Long.MAX_VALUE."));
            /*
            // As governed by rule 3.17, when demand overflows `Long.MAX_VALUE` we treat the signalled demand as "effectively unbounded"
            demand = Long.MAX_VALUE;  // Here we protect from the overflow and treat it as "effectively unbounded"
            handleSend(); // Then we proceed with sending data downstream
             */
        } else {
            demand += n;  // Here we record the downstream demand
            handleSend(); // Then we can proceed with sending data downstream
        }
    }

    // This handles cancellation requests, and is idempotent, thread-safe and not synchronously performing heavy
    // computations as specified in rule 3.5
    protected void handleCancel() {
        cancelled = true;
    }

    // This is our behavior for producing elements downstream
    private void handleSend() {
        try {
            boolean finished;
            if (resultsQueue.peek() != null) {
                do {
                    subscriber.onNext(resultsQueue.poll().get());
                    finished = resultsQueue.peek() == null;
                } while (!finished          // There are more results to consume
                         && !cancelled);    // This makes sure that rule 1.8 is upheld
            }

            if (completed) {             // The batch was completed during the last request
                handleCancel();          // We need to consider this `Subscription` as cancelled as per rule 1.6
                subscriber.onComplete(); // Then we signal `onComplete` as per rule 1.2 and 1.5
            } else if (started && !cancelled && demand > 0) {
                // This makes sure that rule 1.1 is upheld (sending more than was demanded)
                // If the `Subscription` is still alive and well, and we have demand to satisfy,
                // we signal ourselves to send more data
                long requestAmount = demand > batchSize ? batchSize : demand;
                demand -= requestAmount;
                doRequest(requestAmount);  // This makes sure that rule 1.1 is upheld (sending more than was demanded)
            }
        } catch (final MongoException t) {
            // We can only get here if `onNext` or `onComplete` threw, and they are not allowed to according to 2.13,
            // so we can only cancel and log here.
            // Make sure that we are cancelled, since we cannot do anything else since the `Subscriber` is faulty.
            handleCancel();
            LOGGER.error(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onNext"
                    + " or onComplete.", t);
        }
    }

    // This is a helper method to ensure that we always `cancel` when we signal `onError` as per rule 1.6
    private void terminateDueTo(final Throwable t) {
        cancelled = true; // When we signal onError, the subscription must be considered as cancelled, as per rule 1.6
        log("terminated: " + t);
        try {
            subscriber.onError(t); // Then we signal the error downstream, to the `Subscriber`
        } catch (final Throwable t2) {
            // If `onError` throws an exception, this is a spec violation according to rule 1.13, so log it.
            LOGGER.error(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception"
                    + " from onError.", t2);
        }
    }

    // What `signal` does is that it sends signals to the `Subscription` asynchronously
    private void signal(final Signal signal) {
        // Then we try to schedule it for execution, if it isn't already
        if (inboundSignals.offer(signal)) {
            if (started) {
                next();
            }
        }
    }

    // This is the main "event loop" if you so will
    final void next() {
        if (on.compareAndSet(false, true)) { // establishes a happens-before relationship with the end of the previous run
            try {
                while (!inboundSignals.isEmpty()) {
                    final Signal s = inboundSignals.poll(); // We take a signal off the queue
                    if (!cancelled) { // to make sure that we follow rule 1.8, 3.6 and 3.7
                        // Below we simply unpack the `Signal`s and invoke the corresponding methods
                        if (s instanceof Request) {
                            handleRequest(((Request) s).n);
                        } else if (s == Send.Instance) {
                            handleSend();
                        } else if (s == Cancel.Instance) {
                            handleCancel();
                        }
                    }
                }
            } finally {
                on.set(false); // establishes a happens-before relationship with the beginning of the next run
            }

            // If we still have signals to process then process them
            if (!cancelled && !inboundSignals.isEmpty()) {
                next();
            }
        }
    }

    public void onNext(final T t) {
        if (!cancelled) {
            log("onNext - queued");
            resultsQueue.add(new Result<T>(t));
            signal(Send.Instance);
        } else {
            log("onNext - canceled");
        }
    }

    public void onError(final Throwable t) {
        log("onError");
        terminateDueTo(t);  // If calling  we need to treat the stream as errored as per rule 1.4
    }

    public void onComplete() {
        log("onComplete");
        completed = true;
        signal(Send.Instance);
    }


    // Our implementation of `Subscription.request` sends a signal to the Subscription that more elements are in demand
    @Override
    public void request(final long n) {
        signal(new Request(n));
    }

    // Our implementation of `Subscription.cancel` sends a signal to the Subscription that the `Subscriber` is not
    // interested in any more elements
    @Override
    public void cancel() {
        signal(Cancel.Instance);
    }

    // The reason for the `init` method is that we want to ensure the `SubscriptionImpl`
    // is completely constructed before it is exposed to the thread pool, therefor this
    // method is only intended to be invoked once, and immediately after the constructor has
    // finished.
    void start() {
        try {
            log("Subscribing to subscriber");
            subscriber.onSubscribe(this);
        } catch (final Throwable t) { // Due diligence to obey 2.13
            terminateDueTo(new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by "
                    + "throwing an exception from onSubscribe.", t));
        }
        started = true;
        next();
    }

    protected abstract void doRequest(final long n);
    protected String getName() {
        return this.getClass().getSimpleName();
    }
    protected void log(final String msg) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getName() + ": " + msg);
        }
    }

    /**
     * A Result container so we can queue null results
     *
     * @param <R> The type of the result in the container
     */
    class Result<R> {
        private final R result;

        Result(final R result) {
            this.result = result;
        }

        R get() {
            return result;
        }
    }


    // Signal interface for the signal queue
    interface Signal {
    }

    enum Cancel implements Signal {Instance}

    enum Send implements Signal {Instance}

    static final class Request implements Signal {
        private final long n;

        Request(final long n) {
            this.n = n;
        }
    }

}
