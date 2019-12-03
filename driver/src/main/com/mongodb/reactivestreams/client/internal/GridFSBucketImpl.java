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
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper.toCallbackAsyncInputStream;
import static com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper.toCallbackAsyncOutputStream;
import static com.mongodb.reactivestreams.client.internal.PublisherHelper.voidToSuccessCallback;


/**
 * The internal GridFSBucket implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
@SuppressWarnings("deprecation")
public final class GridFSBucketImpl implements GridFSBucket {
    private final com.mongodb.async.client.gridfs.GridFSBucket wrapped;

    /**
     * The GridFSBucket constructor
     *
     * <p>This should not be considered a part of the public API.</p>
     *
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
    public boolean getDisableMD5() {
        return wrapped.getDisableMD5();
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
    public GridFSBucket withDisableMD5(final boolean disableMD5) {
        return new GridFSBucketImpl(wrapped.withDisableMD5(disableMD5));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final String filename) {
        return openUploadStream(filename, new GridFSUploadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(filename, options));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(id, filename, options));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final ClientSession clientSession, final String filename) {
        return openUploadStream(clientSession, filename, new GridFSUploadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final ClientSession clientSession, final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(clientSession.getWrapped(), filename, options));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename) {
        return openUploadStream(clientSession, id, filename, new GridFSUploadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream
    openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return new GridFSUploadStreamImpl(wrapped.openUploadStream(clientSession.getWrapped(), id, filename, options));
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final String filename,
                                                final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source) {
        return uploadFromStream(filename, source, new GridFSUploadOptions());
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final String filename,
                                                final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source,
                                                final GridFSUploadOptions options) {
        return new ObservableToPublisher<ObjectId>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<ObjectId>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<ObjectId> callback) {
                        wrapped.uploadFromStream(filename, toCallbackAsyncInputStream(source), options, callback);
                    }
                }));
    }

    @Override
    public Publisher<Success> uploadFromStream(final BsonValue id, final String filename,
                                               final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source) {
        return uploadFromStream(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public Publisher<Success> uploadFromStream(final BsonValue id, final String filename,
                                               final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source,
                                               final GridFSUploadOptions options) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.uploadFromStream(id, filename, toCallbackAsyncInputStream(source), options,
                                voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final ClientSession clientSession, final String filename,
                                                final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source) {
        return uploadFromStream(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public Publisher<ObjectId> uploadFromStream(final ClientSession clientSession, final String filename,
                                                final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source,
                                                final GridFSUploadOptions options) {
        return new ObservableToPublisher<ObjectId>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<ObjectId>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<ObjectId> callback) {
                        wrapped.uploadFromStream(clientSession.getWrapped(), filename, toCallbackAsyncInputStream(source), options,
                                callback);
                    }
                }));
    }

    @Override
    public Publisher<Success> uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                               final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source) {
        return uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public Publisher<Success> uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                               final com.mongodb.reactivestreams.client.gridfs.AsyncInputStream source,
                                               final GridFSUploadOptions options) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.uploadFromStream(clientSession.getWrapped(), id, filename, toCallbackAsyncInputStream(source), options,
                                voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(id));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream openDownloadStream(final BsonValue id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(id));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream
    openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(filename, options));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream
    openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(clientSession.getWrapped(), id));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream
    openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(clientSession.getWrapped(), id));
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream
    openDownloadStream(final ClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream
    openDownloadStream(final ClientSession clientSession, final String filename, final GridFSDownloadOptions options) {
        return new GridFSDownloadStreamImpl(wrapped.openDownloadStream(clientSession.getWrapped(), filename, options));
    }

    @Override
    public Publisher<Long> downloadToStream(final ObjectId id,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(id, toCallbackAsyncOutputStream(destination), callback);
                    }
                }));
    }


    @Override
    public Publisher<Long> downloadToStream(final BsonValue id,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(id, toCallbackAsyncOutputStream(destination), callback);
                    }
                }));
    }

    @Override
    public Publisher<Long> downloadToStream(final String filename,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return downloadToStream(filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public Publisher<Long> downloadToStream(final String filename,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination,
                                            final GridFSDownloadOptions options) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(filename, toCallbackAsyncOutputStream(destination), options, callback);
                    }
                }));
    }

    @Override
    public Publisher<Long> downloadToStream(final ClientSession clientSession, final ObjectId id,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(clientSession.getWrapped(), id, toCallbackAsyncOutputStream(destination), callback);
                    }
                }));
    }

    @Override
    public Publisher<Long> downloadToStream(final ClientSession clientSession, final BsonValue id,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(clientSession.getWrapped(), id, toCallbackAsyncOutputStream(destination), callback);
                    }
                }));
    }

    @Override
    public Publisher<Long> downloadToStream(final ClientSession clientSession, final String filename,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination) {
        return downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public Publisher<Long> downloadToStream(final ClientSession clientSession, final String filename,
                                            final com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream destination,
                                            final GridFSDownloadOptions options) {
        return new ObservableToPublisher<Long>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                        wrapped.downloadToStream(clientSession.getWrapped(), filename, toCallbackAsyncOutputStream(destination), options,
                                callback);
                    }
                }));
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename, final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename, final Publisher<ByteBuffer> source,
                                                   final GridFSUploadOptions options) {
        return executeUploadFromPublisher(openUploadStream(new BsonObjectId(), filename, options), source).withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Success> uploadFromPublisher(final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Success> uploadFromPublisher(final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(openUploadStream(id, filename, options), source);
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(openUploadStream(clientSession, new BsonObjectId(), filename, options), source)
                .withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Success> uploadFromPublisher(final ClientSession clientSession, final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Success> uploadFromPublisher(final ClientSession clientSession, final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(openUploadStream(clientSession, id, filename, options), source);
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ObjectId id) {
        return executeDownloadToPublisher(openDownloadStream(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final BsonValue id) {
        return executeDownloadToPublisher(openDownloadStream(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename) {
        return executeDownloadToPublisher(openDownloadStream(filename));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename, final GridFSDownloadOptions options) {
        return executeDownloadToPublisher(openDownloadStream(filename, options));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final ObjectId id) {
        return executeDownloadToPublisher(openDownloadStream(clientSession, id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final BsonValue id) {
        return executeDownloadToPublisher(openDownloadStream(clientSession, id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final String filename) {
        return executeDownloadToPublisher(openDownloadStream(clientSession, filename));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final String filename,
                                                     final GridFSDownloadOptions options) {
        return executeDownloadToPublisher(openDownloadStream(clientSession, filename, options));
    }

    private GridFSDownloadPublisher
    executeDownloadToPublisher(final com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream gridFSDownloadStream) {
        return new GridFSDownloadPublisherImpl(gridFSDownloadStream);
    }

    private GridFSUploadPublisherImpl
    executeUploadFromPublisher(final com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream gridFSUploadStream,
                               final Publisher<ByteBuffer> source) {
        return new GridFSUploadPublisherImpl(gridFSUploadStream, source);
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
    public GridFSFindPublisher find(final ClientSession clientSession) {
        return new GridFSFindPublisherImpl(wrapped.find(clientSession.getWrapped()));
    }

    @Override
    public GridFSFindPublisher find(final ClientSession clientSession, final Bson filter) {
        return new GridFSFindPublisherImpl(wrapped.find(clientSession.getWrapped(), filter));
    }

    @Override
    public Publisher<Success> delete(final ObjectId id) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.delete(id, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> delete(final BsonValue id) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.delete(id, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> delete(final ClientSession clientSession, final ObjectId id) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.delete(clientSession.getWrapped(), id, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> delete(final ClientSession clientSession, final BsonValue id) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.delete(clientSession.getWrapped(), id, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> rename(final ObjectId id, final String newFilename) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.rename(id, newFilename, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> rename(final BsonValue id, final String newFilename) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.rename(id, newFilename, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.rename(clientSession.getWrapped(), id, newFilename, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.rename(clientSession.getWrapped(), id, newFilename, voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> drop() {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.drop(voidToSuccessCallback(callback));
                    }
                }));
    }

    @Override
    public Publisher<Success> drop(final ClientSession clientSession) {
        return new ObservableToPublisher<Success>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                        wrapped.drop(clientSession.getWrapped(), voidToSuccessCallback(callback));
                    }
                }));
    }

}
