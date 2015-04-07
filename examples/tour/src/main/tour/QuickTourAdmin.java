/*
 * Copyright 2015 MongoDB, Inc.
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

package tour;

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.text;
import static java.util.Arrays.asList;
import static tour.SubscriberHelpers.ObservableSubscriber;
import static tour.SubscriberHelpers.OperationSubscriber;
import static tour.SubscriberHelpers.PrintDocumentSubscriber;
import static tour.SubscriberHelpers.PrintSubscriber;

/**
 * The QuickTourAdmin code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class QuickTourAdmin {
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     * @throws Throwable if an operation fails
     */
    public static void main(final String[] args) throws Throwable {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create();
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");


        // get a handle to the "test" collection
        MongoCollection<Document> collection = database.getCollection("test");
        ObservableSubscriber subscriber = new ObservableSubscriber<Success>();
        collection.drop().subscribe(subscriber);
        subscriber.await();

        // getting a list of databases
        mongoClient.listDatabaseNames().subscribe(new PrintSubscriber<String>("Database Names: %s"));

        // drop a database
        subscriber = new ObservableSubscriber<Success>();
        mongoClient.getDatabase("databaseToBeDropped").drop().subscribe(subscriber);
        subscriber.await();

        // create a collection
        database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000))
                    .subscribe(new PrintSubscriber<Success>("Creation Created!"));


        database.listCollectionNames().subscribe(new PrintSubscriber<String>("Collection Names: %s"));

        // drop a collection:
        subscriber = new ObservableSubscriber<Success>();
        collection.drop().subscribe(subscriber);
        subscriber.await();

        // create an ascending index on the "i" field
        collection.createIndex(new Document("i", 1)).subscribe(new PrintSubscriber<String>("Created an index named: %s"));

        // list the indexes on the collection
        collection.listIndexes().subscribe(new PrintDocumentSubscriber());


        // create a text index on the "content" field
        subscriber = new PrintSubscriber<String>("Created an index named: %s");
        collection.createIndex(new Document("content", "text")).subscribe(subscriber);
        subscriber.await();

        subscriber = new OperationSubscriber();
        collection.insertMany(asList(new Document("_id", 0).append("content", "textual content"),
                new Document("_id", 1).append("content", "additional content"),
                new Document("_id", 2).append("content", "irrelevant content"))).subscribe(subscriber);
        subscriber.await();

        // Find using the text index
        subscriber = new PrintSubscriber("Text search matches: %s");
        collection.count(text("textual content -irrelevant")).subscribe(subscriber);
        subscriber.await();

        // Find using the $language operator
        subscriber = new PrintSubscriber("Text search matches (english): %s");
        Bson textSearch = text("textual content -irrelevant", "english");
        collection.count(textSearch).subscribe(subscriber);
        subscriber.await();

        // Find the highest scoring match
        System.out.print("Highest scoring document: ");
        Document projection = new Document("score", new Document("$meta", "textScore"));
        collection.find(textSearch).projection(projection).first().subscribe(new PrintDocumentSubscriber());


        // Run a command
        database.runCommand(new Document("buildInfo", 1)).subscribe(new PrintDocumentSubscriber());

        // release resources
        subscriber = new OperationSubscriber();
        database.drop().subscribe(subscriber);
        subscriber.await();
        mongoClient.close();
    }

    private QuickTourAdmin() {
    }
}
