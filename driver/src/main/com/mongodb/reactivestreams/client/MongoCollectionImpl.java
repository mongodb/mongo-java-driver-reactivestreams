/*
 * Copyright 2014 MongoDB, Inc.
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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class MongoCollectionImpl<T> implements MongoCollection<T> {

    private final com.mongodb.async.client.MongoCollection<T> wrapped;

    MongoCollectionImpl(final com.mongodb.async.client.MongoCollection<T> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public Class<T> getDefaultClass() {
        return wrapped.getDefaultClass();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
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
    public <C> MongoCollection<C> withDefaultClass(final Class<C> clazz) {
        return new MongoCollectionImpl<C>(wrapped.withDefaultClass(clazz));
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<T>(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<T>(wrapped.withReadPreference(readPreference));
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<T>(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public Publisher<Long> count() {
        return count(new BsonDocument(), new CountOptions());
    }

    @Override
    public Publisher<Long> count(final Object filter) {
        return count(filter, new CountOptions());
    }

    @Override
    public Publisher<Long> count(final Object filter, final CountOptions options) {
        return new SingleResultPublisher<Long>() {
            @Override
            void execute(final SingleResultCallback<Long> callback) {
                wrapped.count(filter, options, callback);
            }
        };
    }

    @Override
    public <C> DistinctPublisher<C> distinct(final String fieldName, final Class<C> clazz) {
        return new DistinctPublisherImpl<C>(wrapped.distinct(fieldName, clazz));
    }

    @Override
    public FindPublisher<T> find() {
        return find(new BsonDocument(), wrapped.getDefaultClass());
    }

    @Override
    public <C> FindPublisher<C> find(final Class<C> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FindPublisher<T> find(final Object filter) {
        return find(filter, wrapped.getDefaultClass());
    }

    @Override
    public <C> FindPublisher<C> find(final Object filter, final Class<C> clazz) {
        return new FindPublisherImpl<C>(wrapped.find(filter, clazz));
    }

    @Override
    public AggregatePublisher<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <C> AggregatePublisher<C> aggregate(final List<?> pipeline, final Class<C> clazz) {
        return new AggregatePublisherImpl<C>(wrapped.aggregate(pipeline, clazz));
    }

    @Override
    public MapReducePublisher<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, Document.class);
    }

    @Override
    public <C> MapReducePublisher<C> mapReduce(final String mapFunction, final String reduceFunction, final Class<C> clazz) {
        return new MapReducePublisherImpl<C>(wrapped.mapReduce(mapFunction, reduceFunction, clazz));
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        return new SingleResultPublisher<BulkWriteResult>() {
            @Override
            void execute(final SingleResultCallback<BulkWriteResult> callback) {
                wrapped.bulkWrite(requests, options, callback);
            }
        };
    }

    @Override
    public Publisher<Void> insertOne(final T document) {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.insertOne(document, callback);
            }
        };
    }

    @Override
    public Publisher<Void> insertMany(final List<? extends T> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Publisher<Void> insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.insertMany(documents, options, callback);
            }
        };
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Object filter) {
        return new SingleResultPublisher<DeleteResult>() {
            @Override
            void execute(final SingleResultCallback<DeleteResult> callback) {
                wrapped.deleteOne(filter, callback);
            }
        };
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Object filter) {
        return new SingleResultPublisher<DeleteResult>() {
            @Override
            void execute(final SingleResultCallback<DeleteResult> callback) {
                wrapped.deleteMany(filter, callback);
            }
        };
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Object filter, final T replacement) {
        return replaceOne(filter, replacement, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Object filter, final T replacement, final UpdateOptions options) {
        return new SingleResultPublisher<UpdateResult>() {
            @Override
            void execute(final SingleResultCallback<UpdateResult> callback) {
                wrapped.replaceOne(filter, replacement, options, callback);
            }
        };
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Object filter, final Object update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Object filter, final Object update, final UpdateOptions options) {
        return new SingleResultPublisher<UpdateResult>() {
            @Override
            void execute(final SingleResultCallback<UpdateResult> callback) {
                wrapped.updateOne(filter, update, options, callback);
            }
        };
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Object filter, final Object update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Object filter, final Object update, final UpdateOptions options) {
        return new SingleResultPublisher<UpdateResult>() {
            @Override
            void execute(final SingleResultCallback<UpdateResult> callback) {
                wrapped.updateMany(filter, update, options, callback);
            }
        };
    }

    @Override
    public Publisher<T> findOneAndDelete(final Object filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final Object filter, final FindOneAndDeleteOptions options) {
        return new SingleResultPublisher<T>() {
            @Override
            void execute(final SingleResultCallback<T> callback) {
                wrapped.findOneAndDelete(filter, options, callback);
            }
        };
    }

    @Override
    public Publisher<T> findOneAndReplace(final Object filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final Object filter, final T replacement, final FindOneAndReplaceOptions options) {
        return new SingleResultPublisher<T>() {
            @Override
            void execute(final SingleResultCallback<T> callback) {
                wrapped.findOneAndReplace(filter, replacement, options, callback);
            }
        };
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Object filter, final Object update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Object filter, final Object update, final FindOneAndUpdateOptions options) {
        return new SingleResultPublisher<T>() {
            @Override
            void execute(final SingleResultCallback<T> callback) {
                wrapped.findOneAndUpdate(filter, update, options, callback);
            }
        };
    }

    @Override
    public Publisher<Void> dropCollection() {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.dropCollection(callback);
            }
        };
    }

    @Override
    public Publisher<Void> createIndex(final Object key) {
        return createIndex(key, new CreateIndexOptions());
    }

    @Override
    public Publisher<Void> createIndex(final Object key, final CreateIndexOptions options) {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.createIndex(key, options, callback);
            }
        };
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <C> ListIndexesPublisher<C> listIndexes(final Class<C> clazz) {
        return new ListIndexesPublisherImpl<C>(wrapped.listIndexes(clazz));
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName) {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.dropIndex(indexName, callback);
            }
        };
    }

    @Override
    public Publisher<Void> dropIndexes() {
        return dropIndex("*");
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options) {
        return new SingleResultPublisher<Void>() {
            @Override
            void execute(final SingleResultCallback<Void> callback) {
                wrapped.renameCollection(newCollectionNamespace, options, callback);
            }
        };
    }

}
