+++
date = "2015-10-08T09:56:14Z"
title = "Changelog"
[menu.main]
  weight = 55
  pre = "<i class='fa fa-cog'></i>"
+++

## Changelog

Changes between released versions

### 1.5
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.5)

  * Added Collation support to delete operations [JAVARS-27](https://jira.mongodb.org/browse/JAVARS-27)

### 1.4
[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.4)

  * Updated MongoDB Driver Async to 3.4.2
  * Added GridFS support [JAVARS-23](https://jira.mongodb.org/browse/JAVARS-23)
  * Added `MongoClients.getDefaultCodecRegistry()` [JAVARS-16](https://jira.mongodb.org/browse/JAVARS-16)
  * Added a static factory method to MongoClients to taking an already constructed async.client.MongoClient [JAVARS-26](https://jira.mongodb.org/browse/JAVARS-26)


### 1.3

[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARS%20AND%20fixVersion%20%3D%201.3)

  * Updated MongoDB Driver Async to 3.4.0
  * Added Collation support [JAVARS-21](https://jira.mongodb.org/browse/JAVARS-21)
  * Added support for views [JAVARS-22](https://jira.mongodb.org/browse/JAVARS-22)
  * Added support for extending handshake metadata [JAVARS-20](https://jira.mongodb.org/browse/JAVARS-20)

### 1.2

[Full change list](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVARX%20AND%20fixVersion%20%3D%201.2)

  * Updated MongoDB Driver Async to 3.2.0
  * Added support for MongoDB 3.2.0 features

### 1.1 

  * Updated MongoDB Driver Async to 3.1.0
  
    Simplified the driver by using the new `com.mongodb.async.client.Observable` and mapping to `Publisher`

