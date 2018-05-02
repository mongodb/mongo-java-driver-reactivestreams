+++
date = "2015-10-08T09:56:14Z"
title = "Changelog"
[menu.main]
  weight = 55
  pre = "<i class='fa fa-cog'></i>"
+++

## Changelog

Changes between released versions

### 1.8.0
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.8)

  * Updated MongoDB Driver Async to 3.7.0
  * Support first() methods on all MongoIterable based Publishers [JAVARS-66](https://jira.mongodb.org/browse/JAVARS-66)
  * Add support for com.mongodb.MongoClientSettings [JAVARS-60](https://jira.mongodb.org/browse/JAVARS-60)
  * Resolve MongoDriverInformation incompatibility [JAVARS-58](https://jira.mongodb.org/browse/JAVARS-58)
  * Support ReplaceOptions in CRUD API [JAVARS-57](https://jira.mongodb.org/browse/JAVARS-57)
  * Allow configuration of batchSize on FindPublisher and AggregatePublisher [JAVARS-30](https://jira.mongodb.org/browse/JAVARS-30)


### 1.7.1
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.7.1)

  * Updated MongoDB Driver Async to 3.6.3 - Decrease likelihood of implicit session leak [JAVARS-55](https://jira.mongodb.org/browse/JAVARS-55)

### 1.7
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.7)

  * Updated MongoDB Driver Async to 3.6.0
  * MongoDB 3.6 support [JAVARS-41](https://jira.mongodb.org/browse/JAVARS-41)
    See the [what's new in 3.6 guide](http://mongodb.github.io/mongo-java-driver/3.6/whats-new/)

---

### 1.6
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.6)

  * Updated Reactive Streams to 1.0.1 [JAVARS-36](https://jira.mongodb.org/browse/JAVARS-36)
  * Updated MongoDB Driver Async to 3.5.0 [JAVARS-35](https://jira.mongodb.org/browse/JAVARS-35)
  * Deprecated AggregatePublisher#useCursor [JAVARS-34](https://jira.mongodb.org/browse/JAVARS-34)
  * Deprecate modifiers in FindPublisher and replace with properties [JAVARS-28](https://jira.mongodb.org/browse/JAVARS-28)

---

### 1.5
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.5)

  * Added Collation support to delete operations [JAVARS-27](https://jira.mongodb.org/browse/JAVARS-27)

---

### 1.4
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.4)

  * Updated MongoDB Driver Async to 3.4.2
  * Added GridFS support [JAVARS-23](https://jira.mongodb.org/browse/JAVARS-23)
  * Added `MongoClients.getDefaultCodecRegistry()` [JAVARS-16](https://jira.mongodb.org/browse/JAVARS-16)
  * Added a static factory method to MongoClients to taking an already constructed async.client.MongoClient [JAVARS-26](https://jira.mongodb.org/browse/JAVARS-26)

---

### 1.3

[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.3)

  * Updated MongoDB Driver Async to 3.4.0
  * Added Collation support [JAVARS-21](https://jira.mongodb.org/browse/JAVARS-21)
  * Added support for views [JAVARS-22](https://jira.mongodb.org/browse/JAVARS-22)
  * Added support for extending handshake metadata [JAVARS-20](https://jira.mongodb.org/browse/JAVARS-20)

---

### 1.2

[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARX%20AND%20fixVersion%20%3D%201.2)

  * Updated MongoDB Driver Async to 3.2.0
  * Added support for MongoDB 3.2.0 features

---

### 1.1 

  * Updated MongoDB Driver Async to 3.1.0
  
    Simplified the driver by using the new `com.mongodb.async.client.Observable` and mapping to `Publisher`

