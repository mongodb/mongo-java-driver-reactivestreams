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

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.ClusterType.SHARDED;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for asynchronous tests.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static ConnectionString connectionString;
    private static MongoClientImpl mongoClient;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = (MongoClientImpl) MongoClients.create(getConnectionString());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized ConnectionString getConnectionString() {
        if (connectionString == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                    ? DEFAULT_URI : mongoURIProperty;
            connectionString = new ConnectionString(mongoURIString);
        }
        return connectionString;
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static MongoCollection<Document> initializeCollection(final MongoNamespace namespace) throws Throwable {
        MongoDatabase database = getMongoClient().getDatabase(namespace.getDatabaseName());
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            database.executeCommand(new Document("drop", namespace.getCollectionName())).subscribe(subscriber);
            subscriber.await(10, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        }
        return database.getCollection(namespace.getCollectionName());
    }

    public static boolean isSharded() {
        getMongoClient();
        return mongoClient.getCluster().getDescription(10, SECONDS).getType() == SHARDED;
    }

    public static void dropDatabase(final String name) throws Throwable {
        if (name == null) {
            return;
        }
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            getMongoClient().getDatabase(name).executeCommand(new Document("dropDatabase", 1)).subscribe(subscriber);
            subscriber.await(10, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        }
    }

    public static void drop(final MongoNamespace namespace) throws Throwable {
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            getMongoClient().getDatabase(namespace.getDatabaseName())
                    .executeCommand(new Document("drop", namespace.getCollectionName()))
                    .subscribe(subscriber);
            subscriber.await(20, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                dropDatabase(getDefaultDatabaseName());
            } catch (Throwable e) {
                // ignore
            }
            mongoClient.close();
            mongoClient = null;
        }
    }

    public static class ObservableSubscriber<T> implements Subscriber<T> {
        private final List<T> received;
        private final List<Throwable> errors;
        private final CountDownLatch latch;
        private volatile Subscription subscription;
        private volatile boolean completed;

        ObservableSubscriber() {
            this.received = new ArrayList<T>();
            this.errors = new ArrayList<Throwable>();
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
        }

        @Override
        public void onNext(final T t) {
            received.add(t);
        }

        @Override
        public void onError(final Throwable t) {
            errors.add(t);
            onComplete();
        }

        @Override
        public void onComplete() {
            completed = true;
            latch.countDown();
        }

        public Subscription getSubscription() {
            return subscription;
        }

        public List<T> getReceived() {
            return received;
        }

        public List<Throwable> getErrors() {
            return errors;
        }

        public boolean isCompleted() {
            return completed;
        }

        public List<T> get(final long timeout, final TimeUnit unit) throws Throwable {
            return await(timeout, unit).getReceived();
        }

        public ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
            subscription.request(Integer.MAX_VALUE);
            if (!latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
            if (!errors.isEmpty()) {
                throw errors.get(0);
            }
            return this;
        }
    }

}
