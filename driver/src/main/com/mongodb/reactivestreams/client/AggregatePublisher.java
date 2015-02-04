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

import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher for aggregate.
 *
 * @param <T> The type of the result.
 * @since 3.0
 */
public interface AggregatePublisher<T> extends Publisher<T> {

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    AggregatePublisher<T> allowDiskUse(Boolean allowDiskUse);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    AggregatePublisher<T> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets whether the server should use a cursor to return results.
     *
     * @param useCursor whether the server should use a cursor to return results
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    AggregatePublisher<T> useCursor(Boolean useCursor);

    /**
     * Aggregates documents according to the specified aggregation pipeline, which must end with a $out stage.
     *
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    Publisher<Void> toCollection();

}
