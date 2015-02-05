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
import com.mongodb.annotations.ThreadSafe;
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
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * The MongoCollection interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * @param <T> The type that this collection will encode documents from and decode documents to.
 */
@ThreadSafe
public interface MongoCollection<T> {

    /**
     * Gets the namespace of this collection.
     *
     * @return the namespace
     */
    MongoNamespace getNamespace();


    /**
     * Get the default class to cast any documents returned from the database into.
     *
     * @return the default class to cast any documents into
     */
    Class<T> getDefaultClass();

    /**
     * Get the codec registry for the MongoCollection.
     *
     * @return the {@link org.bson.codecs.configuration.CodecRegistry}
     */
    CodecRegistry getCodecRegistry();

    /**
     * Get the read preference for the MongoCollection.
     *
     * @return the {@link com.mongodb.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the write concern for the MongoCollection.
     *
     * @return the {@link com.mongodb.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Create a new MongoCollection instance with a different default class to cast any documents returned from the database into..
     *
     * @param clazz the default class to cast any documents returned from the database into.
     * @param <C>   The type that the new collection will encode documents from and decode documents to
     * @return a new MongoCollection instance with the different default class
     */
    <C> MongoCollection<C> withDefaultClass(Class<C> clazz);

    /**
     * Create a new MongoCollection instance with a different codec registry.
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the collection
     * @return a new MongoCollection instance with the different codec registry
     */
    MongoCollection<T> withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoCollection instance with a different read preference.
     *
     * @param readPreference the new {@link com.mongodb.ReadPreference} for the collection
     * @return a new MongoCollection instance with the different readPreference
     */
    MongoCollection<T> withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoCollection instance with a different write concern.
     *
     * @param writeConcern the new {@link com.mongodb.WriteConcern} for the collection
     * @return a new MongoCollection instance with the different writeConcern
     */
    MongoCollection<T> withWriteConcern(WriteConcern writeConcern);

    /**
     * Counts the number of documents in the collection.
     *
     * @return a publisher with a single element indicating the number of documents
     */
    Publisher<Long> count();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter the query filter
     * @return a publisher with a single element indicating the number of documents
     */
    Publisher<Long> count(Object filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter  the query filter
     * @param options the options describing the count
     * @return a publisher with a single element indicating the number of documents
     */
    Publisher<Long> count(Object filter, CountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @param clazz     the default class to cast any distinct items into.
     * @param <C>       the target type of the iterable.
     * @return a publisher emitting the sequence of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <C> DistinctPublisher<C> distinct(String fieldName, Class<C> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindPublisher<T> find();

    /**
     * Finds all documents in the collection.
     *
     * @param clazz the class to decode each document into
     * @param <C>   the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> FindPublisher<C> find(Class<C> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindPublisher<T> find(Object filter);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @param clazz  the class to decode each document into
     * @param <C>    the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> FindPublisher<C> find(Object filter, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    AggregatePublisher<Document> aggregate(List<?> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param clazz    the class to decode each document into
     * @param <C>      the target document type of the iterable.
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    <C> AggregatePublisher<C> aggregate(List<?> pipeline, Class<C> clazz);


    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MapReducePublisher<Document> mapReduce(String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param clazz          the class to decode each resulting document into.
     * @param <C>            the target document type of the iterable.
     * @return a publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <C> MapReducePublisher<C> mapReduce(String mapFunction, String reduceFunction, Class<C> clazz);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @return a publisher with a single element the BulkWriteResult
     */
    Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends T>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return a publisher with a single element the BulkWriteResult
     */
    Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends T>> requests, BulkWriteOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @return a publisher with a single element indicating when the operation has completed or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<Void> insertOne(T document);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk inserts is to use the BulkWrite API. However, when talking with a
     * server &lt; 2.6, using this method will be faster due to constraints in the bulk API related to error handling.
     *
     * @param documents the documents to insert
     * @return a publisher with a single element indicating when the operation has completed or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<Void> insertMany(List<? extends T> documents);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk inserts is to use the BulkWrite API. However, when talking with a
     * server &lt; 2.6, using this method will be faster due to constraints in the bulk API related to error handling.
     *
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @return a publisher with a single element indicating when the operation has completed or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<Void> insertMany(List<? extends T> documents, InsertManyOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     */
    Publisher<DeleteResult> deleteOne(Object filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     */
    Publisher<DeleteResult> deleteMany(Object filter);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    Publisher<UpdateResult> replaceOne(Object filter, T replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the replace operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    Publisher<UpdateResult> replaceOne(Object filter, T replacement, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Object filter, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter  a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *                registered
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators. This
     *                can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Object filter, Object update, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Object filter, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter  a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *                registered
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators. This
     *                can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Object filter, Object update, UpdateOptions options);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter the query filter to find the document with
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     */
    Publisher<T> findOneAndDelete(Object filter);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     */
    Publisher<T> findOneAndDelete(Object filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<T> findOneAndReplace(Object filter, T replacement);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<T> findOneAndReplace(Object filter, T replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return a publisher with a single element the document that was updated before the update was applied.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<T> findOneAndUpdate(Object filter, Object update);

    /**
     * Atomically find a document and update it.
     *
     * @param filter  a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *                registered
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators. This
     *                can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<T> findOneAndUpdate(Object filter, Object update, FindOneAndUpdateOptions options);

    /**
     * Drops this collection from the Database.
     *
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    Publisher<Void> dropCollection();

    /**
     * Creates an index.
     *
     * @param key an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *            registered
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Publisher<Void> createIndex(Object key);

    /**
     * Creates an index.
     *
     * @param key     an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *                registered
     * @param options the options for the index
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Publisher<Void> createIndex(Object key, CreateIndexOptions options);

    /**
     * Get all the indexes in this collection.
     *
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    ListIndexesPublisher<Document> listIndexes();

    /**
     * Get all the indexes in this collection.
     *
     * @param clazz the class to decode each document into
     * @param <C>   the target document type of the iterable.
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    <C> ListIndexesPublisher<C> listIndexes(Class<C> clazz);

    /**
     * Drops the given index.
     *
     * @param indexName the name of the index to remove
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Publisher<Void> dropIndex(String indexName);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Publisher<Void> dropIndexes();

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Publisher<Void> renameCollection(MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options                the options for renaming a collection
     * @return a publisher with a single element indicating when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Publisher<Void> renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options);

}
