+++
date = "2015-03-17T15:36:56Z"
title = "Admin Quick Tour"
[menu.main]
  parent = "Getting Started"
  identifier = "Admin Quick Tour"
  weight = 50
  pre = "<i class='fa'></i>"
+++

# Admin Quick Tour

This is the second part of the MongoDB driver quick tour. In the
[quick tour]({{< relref "getting-started/quick-tour.md" >}}) we looked at how to
use the Reactive Streams Java driver to execute basic CRUD operations.  In this section we'll look at some of the
administrative features available in the driver.

The following code snippets come from the `QuickTourAdmin.java` example code
that can be found with the [driver
source]({{< srcref "examples/tour/src/main/tour/QuickTourAdmin.java">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Setup

To get started we'll quickly connect and create a `mongoClient`, `database` and `collection`
variable for use in the examples below:

```java
MongoClient mongoClient = new MongoClient(new ConnectionString("mongodb://localhost"));
MongoDatabase database = mongoClient.getDatabase("mydb");
MongoCollection<Document> collection = database.getCollection("test");
```

{{% note %}}
Calling the `getDatabase()` on `MongoClient` does not create a database.
Only when a database is written to will a database be created.  Examples include the creation of an index or the insertion of a document 
into a previously non-existent collection.
{{% /note %}}

## Get A List of Databases

You can get a list of the available databases by calling the `listDatabaseNames` method.  Here we use the `PrintSubscriber` to print the list
of database names:

```java
mongoClient.listDatabaseNames().subscribe(new PrintSubscriber<String>("Database Names: %s"));
```


## Drop A Database

You can drop a database by name using a `MongoClient` instance. Here we wait for the `Publisher` to complete before continuing.

```java
subscriber = new ObservableSubscriber<Success>();
mongoClient.getDatabase("databaseToBeDropped").drop().subscribe(subscriber);
subscriber.await();
```

## Create A Collection

Collections in MongoDB are created automatically simply by inserted a document into it. Using the 
[`createCollection`]({{< apiref "com/mongodb/reactivestreams/client/mongoDatabase.html#createCollection-java.lang.String-com.mongodb.async.SingleResultCallback-">}}) method, 
you can also create a collection explicitly in order to to customize its configuration. For example, to create a capped collection sized to 1 megabyte:

```java
database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000))
    .subscribe(new PrintSubscriber<Success>("Creation Created!"));
```

## Get A List of Collections

You can get a list of the available collections in a database:

```java
database.listCollectionNames().subscribe(new PrintSubscriber<String>("Collection Names: %s"));
```

## Drop A Collection

You can drop a collection by using the drop() method:

```java
subscriber = new ObservableSubscriber<Success>();
collection.drop().subscribe(subscriber);
subscriber.await();
```

## Create An Index

MongoDB supports secondary indexes. To create an index, you just
specify the field or combination of fields, and for each field specify the direction of the index for that field.
For `1` ascending  or `-1` for descending. The following creates an ascending index on the `i` field:

```java
// create an ascending index on the "i" field
collection.createIndex(new Document("i", 1)).subscribe(new PrintSubscriber<String>("Created an index named: %s"));
```

## Get a List of Indexes on a Collection

Use the `listIndexes()` method to get a list of indexes. The following uses the
`PrintDocumentSubscriber` to print the json version of each index document:

```java
subscriber = new PrintDocumentSubscriber()
collection.listIndexes().subscribe(new PrintDocumentSubscriber());
subscriber.await();
```

The example should print the following indexes:

```json
{ "v" : 1, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "mydb.test" }
{ "v" : 1, "key" : { "i" : 1 }, "name" : "i_1", "ns" : "mydb.test" }
```

## Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index specify the string
literal "text" in the index document:

```java
// create a text index on the "content" field
subscriber = new PrintSubscriber<String>("Created an index named: %s");
collection.createIndex(new Document("content", "text")).subscribe(subscriber);
subscriber.await();
```

As of MongoDB 2.6, text indexes are now integrated into the main query
language and enabled by default (here we use the [`Filters.text`]({{< coreapiref "com/mongodb/client/model/Filters.html#text-java.lang.String-">}}) helper):

```java
// Insert some documents
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
```

and it should print:

```json
Text search matches: [2]
Text search matches (english): [2]
Highest scoring document: { "_id" : 1, "content" : "additional content", "score" : 0.75 }
```

For more information about text search see the [text index]({{< docsref "/core/index-text" >}}) and
[$text query operator]({{< docsref "/reference/operator/query/text">}}) documentation.

## Running a command

Not all commands have a specific helper, however you can run any [command]({{< docsref "/reference/command">}})
by using the [`runCommand()`]({{< apiref "com/mongodb/reactivestreams/client/mongoDatabase.html#runCommand-org.bson.conversions.Bson-com.mongodb.ReadPreference-com.mongodb.async.SingleResultCallback-">}}) 
method.  Here we call the [buildInfo]({{ docsref "reference/command/buildInfo" }}) command:

```java
database.runCommand(new Document("buildInfo", 1)).subscribe(new PrintDocumentSubscriber());
```
