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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * The MongoDatabase interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 * @since 1.0
 */
@ThreadSafe
public interface MongoDatabase {
    /**
     * Gets the name of the database.
     *
     * @return the database name
     */
    String getName();

    /**
     * Get the codec registry for the MongoDatabase.
     *
     * @return the {@link org.bson.codecs.configuration.CodecRegistry}
     */
    CodecRegistry getCodecRegistry();

    /**
     * Get the read preference for the MongoDatabase.
     *
     * @return the {@link com.mongodb.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the write concern for the MongoDatabase.
     *
     * @return the {@link com.mongodb.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read concern for the MongoCollection.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 1.2
     * @mongodb.server.release 3.2
     */
    ReadConcern getReadConcern();

    /**
     * Create a new MongoDatabase instance with a different codec registry.
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the collection
     * @return a new MongoDatabase instance with the different codec registry
     */
    MongoDatabase withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoDatabase instance with a different read preference.
     *
     * @param readPreference the new {@link com.mongodb.ReadPreference} for the collection
     * @return a new MongoDatabase instance with the different readPreference
     */
    MongoDatabase withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoDatabase instance with a different write concern.
     *
     * @param writeConcern the new {@link com.mongodb.WriteConcern} for the collection
     * @return a new MongoDatabase instance with the different writeConcern
     */
    MongoDatabase withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new MongoDatabase instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the collection
     * @return a new MongoDatabase instance with the different ReadConcern
     * @since 1.2
     * @mongodb.server.release 3.2
     */
    MongoDatabase withReadConcern(ReadConcern readConcern);

    /**
     * Gets a collection.
     *
     * @param collectionName the name of the collection to return
     * @return the collection
     */
    MongoCollection<Document> getCollection(String collectionName);

    /**
     * Gets a collection, with a specific default document class.
     *
     * @param collectionName the name of the collection to return
     * @param clazz          the default class to cast any documents returned from the database into.
     * @param <TDocument>    the type of the class to use instead of {@code Document}.
     * @return the collection
     */
    <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> clazz);

    /**
     * Executes command in the context of the current database.
     *
     * @param command the command to be run
     * @return a publisher containing the command result
     */
    Publisher<Document> runCommand(Bson command);

    /**
     * Executes command in the context of the current database.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @return a publisher containing the command result
     */
    Publisher<Document> runCommand(Bson command, ReadPreference readPreference);

    /**
     * Executes command in the context of the current database.
     *
     * @param command   the command to be run
     * @param clazz     the default class to cast any documents returned from the database into.
     * @param <TResult> the type of the class to use instead of {@code Document}.
     * @return a publisher containing the command result
     */
    <TResult> Publisher<TResult> runCommand(Bson command, Class<TResult> clazz);

    /**
     * Executes command in the context of the current database.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @param clazz          the default class to cast any documents returned from the database into.
     * @param <TResult>      the type of the class to use instead of {@code Document}.
     * @return a publisher containing the command result
     */
    <TResult> Publisher<TResult> runCommand(Bson command, ReadPreference readPreference, Class<TResult> clazz);

    /**
     * Drops this database.
     *
     * @return a publisher identifying when the database has been dropped
     * @mongodb.driver.manual reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    Publisher<Success> drop();

    /**
     * Gets the names of all the collections in this database.
     *
     * @return a publisher with all the names of all the collections in this database
     */
    Publisher<String> listCollectionNames();

    /**
     * Finds all the collections in this database.
     *
     * @return the fluent list collections interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    ListCollectionsPublisher<Document> listCollections();

    /**
     * Finds all the collections in this database.
     *
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent list collections interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    <TResult> ListCollectionsPublisher<TResult> listCollections(Class<TResult> clazz);

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @return a publisher identifying when the collection has been created
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    Publisher<Success> createCollection(String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName the name for the new collection to create
     * @param options        various options for creating the collection
     * @return a publisher identifying when the collection has been created
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    Publisher<Success> createCollection(String collectionName, CreateCollectionOptions options);

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @return an observable identifying when the collection view has been created
     * @since 1.3
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    Publisher<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline);

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @return an observable identifying when the collection view has been created
     * @since 1.3
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    Publisher<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions);
}
