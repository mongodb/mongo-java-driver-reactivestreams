/*
 * Copyright 2017 MongoDB, Inc.
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

package documentation;


import com.mongodb.ConnectionString;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;


public final class ChangeStreamSamples {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) throws Throwable {
        MongoClient mongoClient;

        // Create a new MongoClient with a MongoDB URI string.
        if (args.length == 0) {
            // Defaults to a localhost replicaset on ports: 27017, 27018, 27019
            mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019"));
        } else {
            mongoClient = MongoClients.create(new ConnectionString(args[0]));
        }

        // Select the MongoDB database.
        MongoDatabase database = mongoClient.getDatabase("testChangeStreams");

        subscribeAndAwait(database.drop());

        // Select the collection to query.
        MongoCollection<Document> collection = database.getCollection("documents");

        /*
         * Example 1
         * Create a simple change stream against an existing collection.
         */
        System.out.println("1. Initial document from the Change Stream:");

        // Create the change stream publisher.
        ChangeStreamPublisher<Document> publisher = collection.watch();

        // Create a subscriber
        ObservableSubscriber<ChangeStreamDocument<Document>> subscriber = new ObservableSubscriber<ChangeStreamDocument<Document>>();
        publisher.subscribe(subscriber);

        // Insert a test document into the collection and request a result
        subscribeAndAwait(collection.insertOne(Document.parse("{username: 'alice123', name: 'Alice'}")));
        subscriber.waitForThenCancel(1);

        /*
         * Example 2
         * Create a change stream with 'lookup' option enabled.
         * The test document will be returned with a full version of the updated document.
         */
        System.out.println("2. Document from the Change Stream, with lookup enabled:");

        // Create the change stream publisher.
        publisher = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
        subscriber = new ObservableSubscriber<ChangeStreamDocument<Document>>();
        publisher.subscribe(subscriber);

        // Update the test document.
        subscribeAndAwait(collection.updateOne(Document.parse("{username: 'alice123'}"),
                Document.parse("{$set : { email: 'alice@example.com'}}")));
        subscriber.waitForThenCancel(1);

        /*
         * Example 3
         * Create a change stream with 'lookup' option using a $match and ($redact or $project) stage.
         */
        System.out.println("3. Document from the Change Stream, with lookup enabled, matching `update` operations only: ");

        // Insert some dummy data.
        subscribeAndAwait(collection.insertMany(asList(Document.parse("{updateMe: 1}"), Document.parse("{replaceMe: 1}"))));

        // Create $match pipeline stage.
        List<Bson> pipeline = singletonList(
                Aggregates.match(
                        Filters.or(
                                Document.parse("{'fullDocument.username': 'alice123'}"),
                                Filters.in("operationType", asList("update", "replace", "delete"))
                        )
                )
        );

        // Create the change stream cursor with $match.
        publisher = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        subscriber = new ObservableSubscriber<ChangeStreamDocument<Document>>(false);
        publisher.subscribe(subscriber);

        // Update the test document.
        subscribeAndAwait(collection.updateOne(Filters.eq("updateMe", 1), Updates.set("updated", true)));

        // Replace the test document.
        subscribeAndAwait(collection.replaceOne(Filters.eq("replaceMe", 1), Document.parse("{replaced: true}")));

        // Delete the test document.
        subscribeAndAwait(collection.deleteOne(Filters.eq("username", "alice123")));

        subscriber.waitForThenCancel(3);

        List<ChangeStreamDocument<Document>> results = subscriber.getResults();
        System.out.println(format("Update operationType: %s %n %-20s %s", results.get(0).getUpdateDescription(), "", results.get(0)));
        System.out.println(format("Replace operationType: %s", results.get(1)));
        System.out.println(format("Delete operationType: %s", results.get(2)));


        /*
         * Example 4
         * Resume a change stream using a resume token.
         */
        System.out.println("4. Document from the Change Stream including a resume token:");

        // Get the resume token from the last document we saw in the previous change stream cursor.
        BsonDocument resumeToken = results.get(2).getResumeToken();
        System.out.println(resumeToken);

        // Pass the resume token to the resume after function to continue the change stream cursor.
        publisher = collection.watch().resumeAfter(resumeToken);
        subscriber = new ObservableSubscriber<ChangeStreamDocument<Document>>();
        publisher.subscribe(subscriber);

        // Insert a test document.
        subscribeAndAwait(collection.insertOne(Document.parse("{test: 'd'}")));

        // Block until the next result is printed
        subscriber.waitForThenCancel(1);
    }


    private static <T> void subscribeAndAwait(final Publisher<T> publisher) throws Throwable {
        ObservableSubscriber<T> subscriber = new ObservableSubscriber<T>(false);
        publisher.subscribe(subscriber);
        subscriber.await();
    }

    private static class ObservableSubscriber<T> implements Subscriber<T> {
        private final CountDownLatch latch;
        private final List<T> results = new ArrayList<T>();
        private final boolean printResults;

        private volatile int minimumNumberOfResults;
        private volatile int counter;
        private volatile Subscription subscription;
        private volatile Throwable error;

        public ObservableSubscriber() {
            this(true);
        }

        public ObservableSubscriber(final boolean printResults) {
            this.printResults = printResults;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
            subscription.request(Integer.MAX_VALUE);
        }

        @Override
        public void onNext(final T t) {
            results.add(t);
            if (printResults) {
                System.out.println(t);
            }
            counter++;
            if (counter >= minimumNumberOfResults) {
                latch.countDown();
            }
        }

        @Override
        public void onError(final Throwable t) {
            error = t;
            System.out.println(t.getMessage());
            onComplete();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        public List<T> getResults() {
            return results;
        }

        public void await() throws Throwable {
            if (!latch.await(10, SECONDS)) {
                throw new MongoTimeoutException("Publisher timed out");
            }
            if (error != null) {
                throw error;
            }
        }

        public void waitForThenCancel(final int minimumNumberOfResults) throws Throwable {
            this.minimumNumberOfResults = minimumNumberOfResults;
            if (minimumNumberOfResults > counter) {
                await();
            }
            subscription.cancel();
        }
    }

    private ChangeStreamSamples() {
    }
}
