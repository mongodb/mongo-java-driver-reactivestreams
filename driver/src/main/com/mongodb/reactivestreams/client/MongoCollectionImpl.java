/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.MapReduceOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final OperationOptions options;
    private final Class<T> clazz;
    private final AsyncOperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz,
                        final OperationOptions options, final AsyncOperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.clazz = notNull("clazz", clazz);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public OperationOptions getOptions() {
        return options;
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
        CountOperation operation = new CountOperation(namespace)
                .filter(asBson(filter))
                .skip(options.getSkip())
                .limit(options.getLimit())
                .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS);
        if (options.getHint() != null) {
            operation.hint(asBson(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        return Publishers.publish(operation, this.options.getReadPreference(), executor);
    }

    @Override
    public Publisher<Object> distinct(final String fieldName, final Object filter) {
        return distinct(fieldName, filter, new DistinctOptions());
    }

    @Override
    public Publisher<Object> distinct(final String fieldName, final Object filter, final DistinctOptions options) {
        DistinctOperation operation = new DistinctOperation(namespace, fieldName)
                .filter(asBson(filter))
                .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS);

        return Publishers.flatten(
                Publishers.map(operation, this.options.getReadPreference(), executor,
                        new Function<BsonArray, List<Object>>() {
                            @Override
                            public List<Object> apply(final BsonArray bsonValues) {
                                List<Object> distinctList = new ArrayList<Object>();
                                for (BsonValue value : bsonValues) {
                                    BsonDocument bsonDocument = new BsonDocument("value", value);
                                    Document document = getOptions().getCodecRegistry().get(Document.class)
                                            .decode(new BsonDocumentReader(bsonDocument),
                                                    DecoderContext.builder().build());
                                    distinctList.add(document.get("value"));
                                }
                                return distinctList;
                            }
                        }));
    }

    @Override
    public FluentFindPublisher<T> find() {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public <C> FluentFindPublisher<C> find(final Class<C> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FluentFindPublisher<T> find(final Object filter) {
        return find(filter, clazz);
    }

    @Override
    public <C> FluentFindPublisher<C> find(final Object filter, final Class<C> clazz) {
        return new FluentFindPublisherImpl<C>(namespace, options, executor, filter, new FindOptions(), clazz);
    }

    @Override
    public Publisher<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, new AggregateOptions(), Document.class);
    }

    @Override
    public <C> Publisher<C> aggregate(final List<?> pipeline, final Class<C> clazz) {
        return aggregate(pipeline, new AggregateOptions(), clazz);
    }

    @Override
    public Publisher<Document> aggregate(final List<?> pipeline, final AggregateOptions options) {
        return aggregate(pipeline, options, Document.class);
    }

    @Override
    public <C> Publisher<C> aggregate(final List<?> pipeline, final AggregateOptions options, final Class<C> clazz) {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);
        BsonValue outCollection = aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                    .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                    .allowDiskUse(options.getAllowDiskUse());

            executor.execute(operation, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    // NoOp
                    // Todo - review api - this is a race. - flatMap?
                }
            });
            return new FluentFindPublisherImpl<C>(new MongoNamespace(namespace.getDatabaseName(), outCollection.asString().getValue()),
                    this.options, executor, new BsonDocument(), new FindOptions(), clazz);
        } else {
            return Publishers.flattenCursor(new AggregateOperation<C>(namespace, aggregateList, getCodec(clazz))
                            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                            .allowDiskUse(options.getAllowDiskUse())
                            .batchSize(options.getBatchSize())
                            .useCursor(options.getUseCursor()),
                    this.options.getReadPreference(),
                    executor);
        }
    }


    @Override
    public Publisher<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions());
    }

    @Override
    public Publisher<Document> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options) {
        return mapReduce(mapFunction, reduceFunction, options, Document.class);
    }

    @Override
    public <C> Publisher<C> mapReduce(final String mapFunction, final String reduceFunction, final Class<C> clazz) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions(), clazz);
    }

    @Override
    public <C> Publisher<C> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options,
                                      final Class<C> clazz) {
        if (options.isInline()) {
            MapReduceWithInlineResultsOperation<C> operation =
                    new MapReduceWithInlineResultsOperation<C>(getNamespace(),
                            new BsonJavaScript(mapFunction),
                            new BsonJavaScript(reduceFunction),
                            getCodec(clazz))
                            .filter(asBson(options.getFilter()))
                            .limit(options.getLimit())
                            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                            .jsMode(options.isJsMode())
                            .scope(asBson(options.getScope()))
                            .sort(asBson(options.getSort()))
                            .verbose(options.isVerbose());
            if (options.getFinalizeFunction() != null) {
                operation.finalizeFunction(new BsonJavaScript(options.getFinalizeFunction()));
            }
            return Publishers.flattenCursor(operation, this.options.getReadPreference(), executor);
        } else {
            MapReduceToCollectionOperation operation =
                    new MapReduceToCollectionOperation(getNamespace(),
                            new BsonJavaScript(mapFunction),
                            new BsonJavaScript(reduceFunction),
                            options.getCollectionName())
                            .filter(asBson(options.getFilter()))
                            .limit(options.getLimit())
                            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                            .jsMode(options.isJsMode())
                            .scope(asBson(options.getScope()))
                            .sort(asBson(options.getSort()))
                            .verbose(options.isVerbose())
                            .action(options.getAction().getValue())
                            .nonAtomic(options.isNonAtomic())
                            .sharded(options.isSharded())
                            .databaseName(options.getDatabaseName());

            if (options.getFinalizeFunction() != null) {
                operation.finalizeFunction(new BsonJavaScript(options.getFinalizeFunction()));
            }
            executor.execute(operation, new SingleResultCallback<MapReduceStatistics>() {
                @Override
                public void onResult(final MapReduceStatistics result, final Throwable t) {
                    // Noop
                    // Todo - this is a race
                }
            });

            String databaseName = options.getDatabaseName() != null ? options.getDatabaseName() : namespace.getDatabaseName();
            OperationOptions readOptions = OperationOptions.builder().readPreference(primary()).build().withDefaults(this.options);
            return new FluentFindPublisherImpl<C>(new MongoNamespace(databaseName, options.getCollectionName()), readOptions, executor,
                    new BsonDocument(), new FindOptions(), clazz);
        }
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        List<WriteRequest> writeRequests = new ArrayList<WriteRequest>(requests.size());
        for (WriteModel<? extends T> writeModel : requests) {
            WriteRequest writeRequest;
            if (writeModel instanceof InsertOneModel) {
                InsertOneModel<T> insertOneModel = (InsertOneModel<T>) writeModel;
                if (getCodec() instanceof CollectibleCodec) {
                    ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(insertOneModel.getDocument());
                }
                writeRequest = new InsertRequest(asBson(insertOneModel.getDocument()));
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<T> replaceOneModel = (ReplaceOneModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(replaceOneModel.getFilter()), asBson(replaceOneModel.getReplacement()),
                        WriteRequest.Type.REPLACE)
                        .upsert(replaceOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel<T> updateOneModel = (UpdateOneModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateOneModel.getFilter()), asBson(updateOneModel.getUpdate()),
                        WriteRequest.Type.UPDATE)
                        .multi(false)
                        .upsert(updateOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<T> updateManyModel = (UpdateManyModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateManyModel.getFilter()), asBson(updateManyModel.getUpdate()),
                        WriteRequest.Type.UPDATE)
                        .multi(true)
                        .upsert(updateManyModel.getOptions().isUpsert());
            } else if (writeModel instanceof DeleteOneModel) {
                DeleteOneModel<T> deleteOneModel = (DeleteOneModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteOneModel.getFilter())).multi(false);
            } else if (writeModel instanceof DeleteManyModel) {
                DeleteManyModel<T> deleteManyModel = (DeleteManyModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteManyModel.getFilter())).multi(true);
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            writeRequests.add(writeRequest);
        }

        return Publishers.publish(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(), this.options.getWriteConcern())
                , executor);
    }

    @Override
    public Publisher<Void> insertOne(final T document) {
        if (getCodec() instanceof CollectibleCodec) {
            ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
        }
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        requests.add(new InsertRequest(asBson(document)));
        return Publishers.map(new InsertOperation(namespace, true, options.getWriteConcern(), requests), executor,
                new Function<WriteConcernResult, Void>() {
                    @Override
                    public Void apply(final WriteConcernResult writeConcernResult) {
                        return null;
                    }
                });
    }

    @Override
    public Publisher<Void> insertMany(final List<? extends T> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Publisher<Void> insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>(documents.size());
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        return Publishers.map(new InsertOperation(namespace, options.isOrdered(), this.options.getWriteConcern(),
                        requests), executor,
                new Function<WriteConcernResult, Void>() {
                    @Override
                    public Void apply(final WriteConcernResult writeConcernResult) {
                        return null;
                    }
                });
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Object filter) {
        return delete(filter, false);
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Object filter) {
        return delete(filter, true);
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Object filter, final T replacement) {
        return replaceOne(filter, replacement, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Object filter, final T replacement, final UpdateOptions updateOptions) {
        UpdateRequest request = new UpdateRequest(asBson(filter), asBson(replacement), WriteRequest.Type.REPLACE)
                .upsert(updateOptions.isUpsert());
        return createUpdateResult(Publishers.publish(new MixedBulkWriteOperation(namespace, asList(request), true,
                options.getWriteConcern()), executor));
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Object filter, final Object update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Object filter, final Object update, final UpdateOptions options) {
        return update(filter, update, options, false);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Object filter, final Object update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Object filter, final Object update, final UpdateOptions options) {
        return update(filter, update, options, true);
    }

    @Override
    public Publisher<T> findOneAndDelete(final Object filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final Object filter, final FindOneAndDeleteOptions options) {
        return Publishers.publish(new FindAndDeleteOperation<T>(namespace, getCodec())
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort())), executor);
    }

    @Override
    public Publisher<T> findOneAndReplace(final Object filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final Object filter, final T replacement, final FindOneAndReplaceOptions options) {
        return Publishers.publish(new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(replacement))
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort()))
                .returnOriginal(options.getReturnOriginal())
                .upsert(options.isUpsert()), executor);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Object filter, final Object update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Object filter, final Object update, final FindOneAndUpdateOptions options) {
        return Publishers.publish(new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(update))
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort()))
                .returnOriginal(options.getReturnOriginal())
                .upsert(options.isUpsert()), executor);
    }

    @Override
    public Publisher<Void> dropCollection() {
        return Publishers.publish(new DropCollectionOperation(namespace), executor);
    }

    @Override
    public Publisher<Void> createIndex(final Object key) {
        return createIndex(key, new CreateIndexOptions());
    }

    @Override
    public Publisher<Void> createIndex(final Object key, final CreateIndexOptions options) {
        return Publishers.publish(new CreateIndexOperation(getNamespace(), asBson(key))
                .name(options.getName())
                .background(options.isBackground())
                .unique(options.isUnique())
                .sparse(options.isSparse())
                .expireAfterSeconds(options.getExpireAfterSeconds())
                .version(options.getVersion())
                .weights(asBson(options.getWeights()))
                .defaultLanguage(options.getDefaultLanguage())
                .languageOverride(options.getLanguageOverride())
                .textIndexVersion(options.getTextIndexVersion())
                .twoDSphereIndexVersion(options.getTwoDSphereIndexVersion())
                .bits(options.getBits())
                .min(options.getMin())
                .max(options.getMax())
                .bucketSize(options.getBucketSize()), executor);
    }

    @Override
    public Publisher<Document> getIndexes() {
        return getIndexes(Document.class);
    }

    @Override
    public <C> Publisher<C> getIndexes(final Class<C> clazz) {
        return Publishers.flattenCursor(new ListIndexesOperation<C>(namespace, getCodec(clazz)), options.getReadPreference(), executor);
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName) {
        return Publishers.publish(new DropIndexOperation(namespace, indexName), executor);
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
        return Publishers.publish(new RenameCollectionOperation(getNamespace(), newCollectionNamespace)
                .dropTarget(options.isDropTarget()), executor);
    }

    private Publisher<DeleteResult> delete(final Object filter, final boolean multi) {
        return Publishers.map(new DeleteOperation(namespace, true, options.getWriteConcern(),
                        asList(new DeleteRequest(asBson(filter)).multi(multi))), executor,
                new Function<WriteConcernResult, DeleteResult>() {
                    @Override
                    public DeleteResult apply(final WriteConcernResult result) {
                        return result.wasAcknowledged() ? DeleteResult.acknowledged(result.getCount())
                                : DeleteResult.unacknowledged();
                    }
                });
    }

    private Publisher<UpdateResult> update(final Object filter, final Object update, final UpdateOptions updateOptions,
                                           final boolean multi) {
        UpdateRequest request = new UpdateRequest(asBson(filter), asBson(update), WriteRequest.Type.UPDATE)
                .upsert(updateOptions.isUpsert()).multi(multi);
        return createUpdateResult(Publishers.publish(new MixedBulkWriteOperation(namespace, asList(request), true,
                options.getWriteConcern()), executor));
    }

    private Publisher<UpdateResult> createUpdateResult(final Publisher<BulkWriteResult> publisher) {
        return Publishers.map(publisher, new Function<BulkWriteResult, UpdateResult>() {
            @Override
            public UpdateResult apply(final BulkWriteResult result) {
                if (result.wasAcknowledged()) {
                    Long modifiedCount = result.isModifiedCountAvailable() ? (long) result.getModifiedCount() : null;
                    BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
                    return UpdateResult.acknowledged(result.getMatchedCount(), modifiedCount, upsertedId);
                } else {
                    return UpdateResult.unacknowledged();
                }
            }
        });
    }

    private Codec<T> getCodec() {
        return getCodec(clazz);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, options.getCodecRegistry());
    }

    private <D> List<BsonDocument> createBsonDocumentList(final List<D> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (D obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return aggregateList;
    }

}
