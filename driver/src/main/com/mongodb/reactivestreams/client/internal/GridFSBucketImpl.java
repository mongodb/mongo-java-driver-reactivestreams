/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.client.Observables.observe;
import static com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper.toCallbackAsyncInputStream;
import static com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper.toCallbackAsyncOutputStream;
import static com.mongodb.reactivestreams.client.internal.PublisherHelper.voidToSuccessCallback;

/**
 * The internal GridFSBucket implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class GridFSBucketImpl implements GridFSBucket {
    private final com.mongodb.async.client.gridfs.GridFSBucket wrapped;

    /**
     * The GridFSBucket constructor
     *
     * <p>This should not be considered a part of the public API.</p>
     * @param wrapped the GridFSBucket
     */
    public GridFSBucketImpl(final com.mongodb.async.client.gridfs.GridFSBucket wrapped) {
        this.wrapped = notNull("GridFSBucket", wrapped);
    }

    @Override
    public String getBucketName() {
        return wrapped.getBucketName();
    }

    @Override
    public int getChunkSizeBytes() {
        return wrapped.getChunkSizeBytes();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(wrapped.withChunkSizeBytes(chunkSizeBytes));
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(wrapped.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new GridFSBucketImpl(wrapped.withReadConcern(readConcern));
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(filename));
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(filename, options));
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(id, filename));
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(id, filename, options));
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final String filename, final AsyncInputStream source) {
        return new ObservableToPublisher<ObjectId>(observe(new Block<SingleResultCallback<ObjectId>>() {
            @Override
            public void apply(final SingleResultCallback<ObjectId> callback) {
                wrapped.uploadFromStream(filename, toCallbackAsyncInputStream(source), callback);
            }
        }));
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final String filename, final AsyncInputStream source, final GridFSUploadOptions options) {
        return new ObservableToPublisher<ObjectId>(observe(new Block<SingleResultCallback<ObjectId>>() {
            @Override
            public void apply(final SingleResultCallback<ObjectId> callback) {
                wrapped.uploadFromStream(filename, toCallbackAsyncInputStream(source), options, callback);
            }
        }));
    }

    @Override
    public Publisher<Success> uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.uploadFromStream(id, filename, toCallbackAsyncInputStream(source), voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public Publisher<Success> uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source,
                                               final GridFSUploadOptions options) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.uploadFromStream(id, filename, toCallbackAsyncInputStream(source), options, voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(id));
    }

    @Override
    public Publisher<Long> downloadToStream(final ObjectId id, final AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(observe(new Block<SingleResultCallback<Long>>() {
            @Override
            public void apply(final SingleResultCallback<Long> callback) {
                wrapped.downloadToStream(id, toCallbackAsyncOutputStream(destination), callback);
            }
        }));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(id));
    }

    @Override
    public Publisher<Long> downloadToStream(final BsonValue id, final AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(observe(new Block<SingleResultCallback<Long>>() {
            @Override
            public void apply(final SingleResultCallback<Long> callback) {
                wrapped.downloadToStream(id, toCallbackAsyncOutputStream(destination), callback);
            }
        }));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(filename));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(filename, options));
    }

    @Override
    public Publisher<Long> downloadToStream(final String filename, final AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(observe(new Block<SingleResultCallback<Long>>() {
            @Override
            public void apply(final SingleResultCallback<Long> callback) {
                wrapped.downloadToStream(filename, toCallbackAsyncOutputStream(destination), callback);
            }
        }));
    }

    @Override
    public Publisher<Long> downloadToStream(final String filename, final AsyncOutputStream destination,
                                            final GridFSDownloadOptions options) {
        return new ObservableToPublisher<Long>(observe(new Block<SingleResultCallback<Long>>() {
            @Override
            public void apply(final SingleResultCallback<Long> callback) {
                wrapped.downloadToStream(filename, toCallbackAsyncOutputStream(destination), options, callback);
            }
        }));
    }

    @Override
    public GridFSFindPublisher find() {
        return new GridFSFindPublisherImpl(wrapped.find());
    }

    @Override
    public GridFSFindPublisher find(final Bson filter) {
        return new GridFSFindPublisherImpl(wrapped.find(filter));
    }

    @Override
    public Publisher<Success> delete(final ObjectId id) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.delete(id, voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public Publisher<Success> delete(final BsonValue id) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.delete(id, voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public Publisher<Success> rename(final ObjectId id, final String newFilename) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.rename(id, newFilename, voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public Publisher<Success> rename(final BsonValue id, final String newFilename) {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.rename(id, newFilename, voidToSuccessCallback(callback));
            }
        }));
    }

    @Override
    public Publisher<Success> drop() {
        return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
            @Override
            public void apply(final SingleResultCallback<Success> callback) {
                wrapped.drop(voidToSuccessCallback(callback));
            }
        }));
    }

}
