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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.ListCollectionNamesOperation;
import org.bson.BsonDocument;
import org.bson.Document;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.BsonDocumentWrapper.asBsonDocument;

class MongoDatabaseImpl implements MongoDatabase {
    private final OperationOptions options;
    private final String name;
    private final AsyncOperationExecutor executor;

    MongoDatabaseImpl(final String name, final OperationOptions options, final AsyncOperationExecutor executor) {
        this.name = name;
        this.options = options;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, OperationOptions.builder().build());
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final OperationOptions options) {
        return getCollection(collectionName, Document.class, options);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, OperationOptions.builder().build());
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz,
                                                final OperationOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, options.withDefaults(this.options),
                                          executor);
    }

    @Override
    public MongoPublisher<Void> dropDatabase() {
        return Publishers.publish(new DropDatabaseOperation(name), executor);
    }

    @Override
    public MongoPublisher<String> getCollectionNames() {
        return Publishers.flatten(new ListCollectionNamesOperation(name), primary(), executor);
    }

    @Override
    public MongoPublisher<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public MongoPublisher<Void> createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        return Publishers.publish(new CreateCollectionOperation(name, collectionName)
                                  .capped(createCollectionOptions.isCapped())
                                  .sizeInBytes(createCollectionOptions.getSizeInBytes())
                                  .autoIndex(createCollectionOptions.isAutoIndex())
                                  .maxDocuments(createCollectionOptions.getMaxDocuments())
                                  .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes())
                                  .storageEngineOptions(asBson(createCollectionOptions.getStorageEngineOptions())), executor);
    }

    @Override
    public MongoPublisher<Document> executeCommand(final Object command) {
        return executeCommand(command, Document.class);
    }

    @Override
    public MongoPublisher<Document> executeCommand(final Object command, final ReadPreference readPreference) {
        return executeCommand(command, readPreference, Document.class);
    }

    @Override
    public <T> MongoPublisher<T> executeCommand(final Object command, final Class<T> clazz) {
        notNull("command", command);
        return Publishers.publish(new CommandWriteOperation<T>(getName(), asBson(command), options.getCodecRegistry().get(clazz)),
                                  executor);
    }

    @Override
    public <T> MongoPublisher<T> executeCommand(final Object command, final ReadPreference readPreference, final Class<T> clazz) {
        notNull("command", command);
        notNull("readPreference", readPreference);
        return Publishers.publish(new CommandReadOperation<T>(getName(), asBson(command), options.getCodecRegistry().get(clazz)),
                                  readPreference, executor);
    }

    @Override
    public OperationOptions getOptions() {
        return options;
    }

    private BsonDocument asBson(final Object document) {
        return asBsonDocument(document, options.getCodecRegistry());
    }
}
