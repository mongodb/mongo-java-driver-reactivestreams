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

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;
import static tour.SubscriberHelpers.ObservableSubscriber;
import static tour.SubscriberHelpers.OperationSubscriber;
import static tour.SubscriberHelpers.PrintDocumentSubscriber;
import static tour.SubscriberHelpers.PrintSubscriber;


/**
 * The QuickTour code example see: https://mongodb.github.io/mongo-java-driver-reactivestreams/1.0/getting-started
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class QuickTour {

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

        // drop all the data in it
        ObservableSubscriber subscriber = new ObservableSubscriber<Success>();
        collection.drop().subscribe(subscriber);
        subscriber.await();

        // make a document and insert it
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("info", new Document("x", 203).append("y", 102));

        collection.insertOne(doc).subscribe(new OperationSubscriber<Success>());

        // get it (since it's the only one in there since we dropped the rest earlier on)
        collection.find().first().subscribe(new PrintDocumentSubscriber());

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("i", i));
        }

        subscriber = new ObservableSubscriber<Success>();
        collection.insertMany(documents).subscribe(subscriber);
        subscriber.await();

        collection.count().subscribe(new PrintSubscriber<Long>("total # of documents after inserting 100 small ones (should be 101): %s"));

        subscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(subscriber);
        subscriber.await();

        subscriber = new PrintDocumentSubscriber();
        collection.find().subscribe(subscriber);
        subscriber.await();

        // Query Filters
        // now use a query to get 1 document out
        collection.find(eq("i", 71)).first().subscribe(new PrintDocumentSubscriber());

        // now use a range query to get a larger subset
        collection.find(gt("i", 50)).subscribe(new PrintDocumentSubscriber());

        // range query with multiple constraints
        collection.find(and(gt("i", 50), lte("i", 100))).subscribe(new PrintDocumentSubscriber());

        // Sorting
        collection.find(exists("i")).sort(descending("i")).first().subscribe(new PrintDocumentSubscriber());

        // Projection
        collection.find().projection(excludeId()).first().subscribe(new PrintDocumentSubscriber());

        // Update One
        collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)))
                .subscribe(new PrintSubscriber<UpdateResult>("Update Result: %s"));


        // Update Many
        subscriber = new PrintSubscriber<UpdateResult>("Update Result: %s");
        collection.updateMany(lt("i", 100), new Document("$inc", new Document("i", 100))).subscribe(subscriber);
        subscriber.await();

        // Delete One
        collection.deleteOne(eq("i", 110)).subscribe(new PrintSubscriber<DeleteResult>("Delete Result: %s"));

        // Delete Many
        collection.deleteMany(gte("i", 100)).subscribe(new PrintSubscriber<DeleteResult>("Delete Result: %s"));

        subscriber = new ObservableSubscriber<Success>();
        collection.drop().subscribe(subscriber);
        subscriber.await();

        // ordered bulk writes
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        writes.add(new InsertOneModel<Document>(new Document("_id", 4)));
        writes.add(new InsertOneModel<Document>(new Document("_id", 5)));
        writes.add(new InsertOneModel<Document>(new Document("_id", 6)));
        writes.add(new UpdateOneModel<Document>(new Document("_id", 1), new Document("$set", new Document("x", 2))));
        writes.add(new DeleteOneModel<Document>(new Document("_id", 2)));
        writes.add(new ReplaceOneModel<Document>(new Document("_id", 3), new Document("_id", 3).append("x", 4)));

        subscriber = new PrintSubscriber<BulkWriteResult>("Bulk write results: %s");
        collection.bulkWrite(writes).subscribe(subscriber);
        subscriber.await();

        subscriber = new ObservableSubscriber<Success>();
        collection.drop().subscribe(subscriber);
        subscriber.await();

        subscriber = new PrintSubscriber<BulkWriteResult>("Bulk write results: %s");
        collection.bulkWrite(writes, new BulkWriteOptions().ordered(false)).subscribe(subscriber);
        subscriber.await();

        subscriber = new PrintDocumentSubscriber();
        collection.find().subscribe(subscriber);
        subscriber.await();

        // Clean up
        subscriber = new PrintSubscriber("Collection Dropped");
        collection.drop().subscribe(subscriber);
        subscriber.await();

        // release resources
        mongoClient.close();
    }

    private QuickTour() {
    }
}
