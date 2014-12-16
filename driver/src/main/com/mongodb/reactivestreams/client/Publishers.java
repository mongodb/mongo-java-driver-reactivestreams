/*
 * Copyright 2014 the original author or authors.
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

import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.operation.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import org.reactivestreams.Publisher;

import java.util.List;

final class Publishers {

    static <I, O> MongoPublisher<O> map(final Publisher<I> input, final Function<? super I, ? extends O> function) {
        return new MapPublisher<I, O>(input, function);
    }

    static <T> MongoPublisher<T> flatten(final Publisher<List<T>> publisher) {
        return new FlattenPublisher<T>(publisher);
    }

    static <T> MongoPublisher<T> flatten(final AsyncReadOperation<List<T>> operation, final ReadPreference readPreference,
                                         final AsyncOperationExecutor executor) {
        return new FlattenPublisher<T>(publish(operation, readPreference, executor));
    }

    static <T> MongoPublisher<T> publish(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                                         final AsyncOperationExecutor executor) {
        return new ReadOperationPublisher<T>(operation, readPreference, executor);
    }

    static <T> MongoPublisher<T> publish(final AsyncWriteOperation<T> operation, final AsyncOperationExecutor executor) {
        return new WriteOperationPublisher<T>(operation, executor);
    }

    static <T> MongoPublisher<T> flattenCursor(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation,
                                               final ReadPreference readPreference, final AsyncOperationExecutor executor) {
        return new FlattenPublisher<T>(Publishers.publishCursor(operation, readPreference, executor));
    }

    static <T> AsyncBatchCursorPublisher<T> publishCursor(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation,
                                                          final ReadPreference readPreference, final AsyncOperationExecutor executor) {
        return new AsyncBatchCursorPublisher<T>(operation, readPreference, executor);
    }

    private Publishers() {
    }
}
