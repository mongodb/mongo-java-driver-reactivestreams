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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoIterable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class MongoIterablePublisher<TResult> implements Publisher<TResult> {

    private final MongoIterable<TResult> mongoIterable;

    MongoIterablePublisher(final MongoIterable<TResult> mongoIterable) {
        this.mongoIterable = mongoIterable;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new AsyncBatchCursorSubscription(s).start();
    }

    private class AsyncBatchCursorSubscription extends SubscriptionSupport<TResult> {

        private final Lock lock = new ReentrantLock(false);

        private boolean requestedBatchCursor;
        private boolean isReading;
        private boolean isProcessing;
        private boolean cursorCompleted;

        private long wanted = 0;
        private volatile AsyncBatchCursor<TResult> batchCursor = null;
        private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

        public AsyncBatchCursorSubscription(final Subscriber<? super TResult> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            lock.lock();
            boolean mustGetCursor = false;
            try {
                wanted += n;
                if (!requestedBatchCursor) {
                    requestedBatchCursor = true;
                    mustGetCursor = true;
                }
            } finally {
                lock.unlock();
            }

            if (mustGetCursor) {
                getBatchCursor();
            } else {
                processResultsQueue();
            }
        }

        @Override
        protected void handleCancel() {
            super.handleCancel();
            if (batchCursor != null) {
                batchCursor.close();
            }
        }

        private void processResultsQueue() {
            log("processResultsQueue");
            lock.lock();
            boolean mustProcess = false;
            try {
                if (!isProcessing) {
                    isProcessing = true;
                    mustProcess = true;
                }
            } finally {
                lock.unlock();
            }

            if (mustProcess) {
                log("processing");
                boolean getNextBatch = false;

                long processedCount = 0;
                boolean completed = false;
                while (true) {
                    long localWanted = 0;
                    lock.lock();
                    try {
                        wanted -= processedCount;
                        if (resultsQueue.isEmpty()) {
                            completed = cursorCompleted;
                            getNextBatch = wanted > 0;
                            isProcessing = false;
                            break;
                        } else if (wanted == 0) {
                            isProcessing = false;
                            break;
                        }
                        localWanted = wanted;
                    } finally {
                        lock.unlock();
                    }

                    while (localWanted > 0) {
                        TResult item = resultsQueue.poll();
                        if (item == null) {
                            break;
                        } else {
                            onNext(item);
                            localWanted -= 1;
                            processedCount += 1;
                        }
                    }
                }

                if (completed) {
                    onComplete();
                } else if (getNextBatch) {
                    getNextBatch();
                }
            }
        }

        private void getNextBatch() {
            log("getNextBatch");

            lock.lock();
            boolean mustRead = false;
            try {
                if (!isReading) {
                    isReading = true;
                    mustRead = true;
                }
            } finally {
                lock.unlock();
            }

            if (mustRead) {
                batchCursor.setBatchSize(getBatchSize());
                batchCursor.next(new SingleResultCallback<List<TResult>>() {
                    @Override
                    public void onResult(final List<TResult> result, final Throwable t) {
                        lock.lock();
                        try {
                            isReading = false;
                            if (t == null && result == null) {
                                cursorCompleted = true;
                            }
                        } finally {
                            lock.unlock();
                        }

                        if (t != null) {
                            onError(t);
                        } else {
                            if (result != null) {
                                resultsQueue.addAll(result);
                            }
                            processResultsQueue();
                        }
                    }
                });
            }
        }

        private void getBatchCursor() {
            log("getBatchCursor");
            mongoIterable.batchSize(getBatchSize());
            mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
                @Override
                public void onResult(final AsyncBatchCursor<TResult> result, final Throwable t) {
                    if (t != null) {
                        onError(t);
                    } else if (result != null) {
                        batchCursor = result;
                        getNextBatch();
                    } else {
                        onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                    }
                }
            });
        }

        /**
         * Returns the batchSize to be used with the cursor.
         *
         * <p>Anything less than 2 would close the cursor so that is the minimum batchSize and `Integer.MAX_VALUE` is the maximum
         * batchSize.</p>
         *
         * @return the batchSize to use
         */
        private int getBatchSize() {
            long requested = wanted;
            if (requested <= 1) {
                return 2;
            } else if (requested < Integer.MAX_VALUE) {
                return (int) requested;
            } else {
                return Integer.MAX_VALUE;
            }
        }
    }

}
