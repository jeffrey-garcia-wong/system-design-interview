# System Design Interview

## Table of Contents
- Warm-up 
- Mongo DB 
- Kafka 
- Zookeeper 
- Redis Caching 
- Event Drive Architecture
- Active-Active Architecture 
- P, NP, NP-Complete and NP-Hard Problems

<hr>

## Warm-up
`The Mentality`
`Back of the Envelope`
`Full-Stack`

### CAP Theorem
- C
  > The system has only one node and therefore data consistency (C) is not a concern, but it doesn’t provide high availability (A) over networked nodes (P)
- AP
  > The system has multiple nodes across the network (P) to provides high availability (A), but then data consistency across the nodes cannot be achieved (C), as data replication over the networked node is susceptible to delay and failure, consistency cannot be guaranteed. Data will only be eventually consistent across all the nodes when network is resumed.
- CP
  > The system has multiple nodes across the network (P), however in favour of data consistency (C), it needs to stop accepting write requests (give up A) before data synchronisation is completed across all the nodes
- AC
  > Theoretically doesn’t exist because high availability (A) requires partition tolerance (P), but once networked nodes are introduced, consistency cannot be guaranteed unless we give-up availability, or we have to tradeoff consistency for high availability (see AP and CP above).
- ACP
  > The system has multiple nodes across the network (P) for the sake of availability (A), however in favour of data consistency (C), it can only accept write on a single node (A is limited) AND forced to use a synchronous replication to all the networked nodes (P), with a tradeoff in performance (write latency). Synchronous replication must be atomic (rollback all everything if replication timeout) to achieve data consistency across all replica.
  
### Data-Driven Requirements
One way to visualize a system is how its data is shaped and how it flows. 
Here are a some useful factors to think about:

- Working data size
    > Amount of data the system need to deal with for typical operation
- Average request size
    > How much data have to be sent over the network to serve 1 user request?
- Request rate
    > How many incoming requests are expected per user for a given amount of time? 
      How many concurrent users are there at peak (the busiest period)?
- Mutation rate
    > How many incoming (write) requests are expected per user for a given amount of time?
- Consistency
    > How quickly does a mutation have to spread through the system? 
      The faster the better data consistency.
- Locality
    > This has to do with the probability that a user will read item B if they read item A.
      Knowing that a user session is restricted to a well-defined subset of the data allows 
      you to shard/partition it.
- Computation
    > What kinds of math do you need to run on the data before it goes out?
      Can it be precomputed and cached? 
- Latency
    > How quickly are requests supposed to return the result to user? 
- Contention
    > What are the fundamental bottlenecks will be anticipated?
      An application with strict consistency requirements and a high mutation 
      rate might be limited by lock contention.
  
#### Example 1
Let's say we want to build a movies-on-demand service like Netflix or Hulu.<p/>
First, we make assumptions to build a MVP with limited features
> Videos are professionally produced and 20 and 200 minutes long. You want to support 
  a library of 100,000 (10^5) films and 10^5 concurrent users. For simplicity's sake 
  we'll consider only the actual watching of movies and disregard browsing the website, 
  video encoding, user comments & ratings, logs analysis, etc.

Solution: Apply the model above to visualise the requirements in terms of data.

#### References
- [Facebook - the full stack](https://www.facebook.com/notes/10158791462547200/)
- [Google - Numbers you should know](https://static.googleusercontent.com/media/research.google.com/en//people/jeff/stanford-295-talk.pdf)

### Non-Functional Requirements
Classic design consideration for distributed system.
- Capacity
    - Requests volume drives scalability need.
- Performance
    - Horizontal scaling improves read performance but introduce challenges in data consistency for write requests.
- Durability
- Consistency
    - To achieve data consistency system needs to compromise write performance.
- Availability
- Scalability
- Resiliency
- Security 

<hr>

## MongoDB
`NoSQL`
`Document DB`
`Schema Design`
`Embedded Document`
`Sharding`
`Replica Set`
`Transaction`
`Read / Write Concern`
`Quorum`
`Eventual Consistency`
`Causal Consistency`

### [Basics]()
- Cluster (also called replica set) are for redundancy, not scalability.
- You need a minimum of 3 nodes in a cluster, to achieve quorum.
  - The elected primary node, which is responsible for both read and write.
  - The secondary node, which receive the replicated data and stand-by for failure.
    - Read requests are *distributed* between each of the secondary nodes, with the tradeoff of partially inconsistent view of data before replication of data catch-up with primary node.
  - Automatically go into read-only mode if number of available nodes doesn't achieve quorum.
- Each node in the cluster holds a complete copy of all the data in the database.
- Reads and writes to the primary node are guaranteed to be strongly consistent.
- Updates to a single document is always atomic.
- Supports transaction across multiple documents and collections since v4.0.
  - Even supports transaction across shards in a sharded cluster since v4.2.
  - Should not be used frequently if document schema is designed properly.
  - There will be performance degradation.
- Sharding is when you divide your data and put each piece in a different replica set or cluster.
  - It is a way for storing huge data set that cannot fit into the maximum amount of RAM can be installed on a single computer.
  - For a minimal sharded cluster (only 2 shards) with quorum, at least 8 servers are needed, this number goes up by 3 for every shard added.
  - It's often cheaper and easier to simply upgrade hardware (mostly RAM).
- Another use case of sharing is to improve locality where data is stored geographically closer to the user's location, to reduce latency.
  - A shard can associate with multiple zones
  - A zone can associate with multiple shards

#### References:
- [NoSQL Explained](https://www.mongodb.com/resources/basics/databases/nosql-explained)

### [Schema Design Approaches]()
When designing a schema, we want to take into consideration the following:
- Store the data
- Provide good query performance
- Require reasonable amount of hardware

#### Embedding vs. Referencing:
Embedding:<br/>

| Pros                                                        | Cons                                                                                                |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| can retrieve all relevant information in a single query     | overhead with large document                                                                        |
| avoid implementing joins in application code                | embedding too much data inside a single document could potentially hit document maximum size limit. |
| can update related information as a single atomic operation |                                                                                                     |

Referencing:<br/>

| Pros                                                           | Cons                                                                                 |
|----------------------------------------------------------------|--------------------------------------------------------------------------------------|
| can have smaller documents by splitting data                   | read / write for all the data in referenced documents require a minimum of 2 queries |
| less likely to hit document's size limit                       |                                                                                      |
| avoid frequently accessing information not needed by the query |                                                                                      |
| reduced amount of duplicated data                              |                                                                                      |

> duplication of data is not necessarily bad as long it results in better schema 
  (when you can avoid updating duplicated data in lots of documents)

General Guidance:<br/>
1. Favour embedding unless there is a compelling reason not to
   - Embedding all the past addresses belonging to an individual in the Person document
   - Embedding all the past jobs belonging to an individual in the Employee document
2. Needing to access an object on its own is a compelling reason not to embed it
   - Referencing all the parts composing a product in the Product document as an array of parts
    > Application need to query what are the parts needed by a particular product
   - Referencing all the products a part belonging to in the Part document as an array of products
    > Application need to query what are the products that need the same part
   - Each product has a sub-array of linked parts, and each part has a sub-array of products
    > Many-to-many relationship is achievable but update will require modifying 2 documents in different collections in an atomic operation
3. Avoid joins/lookups if possible
    > Don't be afraid of splitting data into different collections if it actually produces a better schema design, where you can avoid updating duplicated data in lots of documents. i.e. Product document that embed every part's object rather than reference to an array of part's id.  
    - Retrieve information from 2 different collections together require to use $lookup to join the data together
    - `$lookup` operations can be expensive, so it's important to consider how "frequent" you'll need to perform `$lookup` if you choose this option.
    - If we find ourselves frequently using `$lookup`, another option is to use the *extended reference pattern*.
      > Instead of embedding all the information or including a reference to JOIN the information, we only embed those fields of the highest priority and most frequently accessed, this works well if the data that is stored in the main document are fields that don't frequently change.
4. Arrays should not grow without bound 
   - If there are more than a couple of hundred documents on the "many" side, don't embed them
   - If there are more than a few thousand documents on the "many" side, don't use an array of ObjectID references. 
   - High-cardinality arrays are a compelling reason not to embed references.
   > Which data to be referenced can also depend on whether the number of the referenced data is unbounded.
   > If the data referenced data is unbounded, storing the reference may exceed document's size limit. 
5. How you model your data depends – entirely – on your particular application's data access patterns. 
   > You want to structure your data to match the ways that your application queries and updates it.
   
#### Summary: <br/>
- One-to-One - Prefer key value pairs within the document
- One-to-Few - Prefer embedding
- One-to-Many - Prefer embedding
- One-to-Squillions - Prefer Referencing
- Many-to-Many - Prefer Referencing

#### References:
- [Schema Design Best Practices](https://www.mongodb.com/developer/products/mongodb/mongodb-schema-design-best-practices/)
- [Schema Design Anti-Pattern](https://www.mongodb.com/developer/products/mongodb/schema-design-anti-pattern-summary/)

### Sharding
#### Benefits of Sharding
- Increased read/write throughput
- High availability
- Increased storage capacity
- Data locality

#### Consideration of Shard Key
Key criteria, ensure data is distributed evenly across shards, or new shards can be added and won't be bounded by the limitation of the shard keys itself.
- the cardinality of the shard key
- the frequency with which shard key values occur
- whether a potential shard key grows monotonically
- Sharding Query Patterns
- Shard Key Limitations
> The shard key value has a direct impact on the cluster's performance and should be chosen carefully. A poorly chosen shard key can lead to performance or scaling issues due to uneven chunk distribution.

#### Sharding Strategy
The type of strategy used can depend on query patterns, application use cases, and data distribution patterns.
- Ranged sharding
    > Ranged sharding is most efficient when the shard key displays the below characteristics.
    - High shard key cardinality 
      - large number of different values so it won't limit the number of shard
      - if number of shards are limited then the system cannot further scale-out even more shards are added
    - Low shard key frequency
      - avoid un-even distribution of data into particular shards
    - Non-monotonically changing shard keys
      - avoid un-even distribution of data into particular shards
    > A range of shard keys whose values are “close” are more likely to reside on the same chunk. This allows for targeted operations as a mongos can route the operations to only the shards that contain the required data.

- Hashed sharding
    - High shard key cardinality
      - large number of different values so it won't limit the number of shards
      - imply low shard key frequency so data is more evenly distributed
    - Ideal for shard keys with fields that change monotonically
    > Data distribution based on hashed values facilitates more even data distribution, especially in data sets where the shard key changes monotonically. However, hashed sharding does not provide efficient range-based operations.

- Zone sharding
    > Zone sharding organizes data into different zones, depending on the application requirements. For example, you may want to store data of all Europe users together, so you can create a zone which has data of all Europe customers. Each zone can be associated with one or more shards, and each shard can have data of one or more zones.

#### Sharding Query Patterns
Because each node only stores part of the data, for each request, the database 
queries need to determine which node or nodes contain the relevant data.

If the data is stored across multiple nodes, the reads and writes could be done 
in parallel. For large-volume data reads, performance is improved because each 
node can read its section of data in parallel with the other nodes.

There is an overhead to reading from multiple nodes. The data from all the nodes 
still needs to be transferred over the network and then combined into a query 
result set. For small data reads, the network latency could be a significant 
portion of the overall query time. For those scenarios, it's more efficient to 
query using `targeted operations` instead of `broadcast operations`.

| Targeted operations |
|---------------------|
| insertOne()         |
| updateOne()         |
| replaceOne()        |
| deleteOne()         |

| Broadcast operations |
|----------------------|
| insertMany()         |
| updateMany()         |
| deleteMany()         |

*Generally, the fastest queries in a sharded environment are those that mongos route to a single shard, using the shard key and the cluster meta data from the config server.*

> The ideal shard key distributes data evenly across the sharded cluster while also facilitating common query patterns. When you choose a shard key, consider your most common query patterns and whether a given shard key covers them.

#### References
- [Scaling](https://www.mongodb.com/resources/basics/scaling)
- [Sharding](https://www.mongodb.com/resources/products/capabilities/sharding)
- [Sharding Strategy](https://www.mongodb.com/docs/manual/sharding/#sharding-strategy)
- [Choosing a Shard Key](https://www.mongodb.com/docs/manual/core/sharding-choose-a-shard-key)
- [Partition Tolerance](https://www.mongodb.com/docs/manual/core/sharding-data-partitioning/)
- [Targeted Query vs Broadcast Query](https://www.mongodb.com/docs/manual/core/sharded-cluster-query-router/#targeted-operations-vs.-broadcast-operations)

### Replication
Replication provides redundancy and increases data availability. With multiple copies 
of data on different database servers, replication provides a level of fault tolerance 
against the loss of a single database server.

Replication also provides increased read capacity as clients can send read operations 
to different servers. Maintaining copies of data in different data centers can increase 
data locality and availability for distributed applications.

The primary node receives all write operations. A replica set can have only one primary 
capable of confirming writes with `{ w: "majority" }` write concern; although in some 
circumstances, another mongod instance may transiently believe itself to also be primary.

The secondary nodes replicate the primary's oplog and apply the operations to their data 
sets such that the secondaries' data sets reflect the primary's data set. If the primary 
is unavailable, an eligible secondary will hold an election to elect itself the new primary.

Secondary nodes replicate the primary's oplog and apply the operations to their data sets 
asynchronously. Replication lag is a delay between an operation on the primary and the 
application of that operation from the oplog to the secondary. Some small delay period 
may be acceptable, but significant problems emerge as replication lag grows, including 
building cache pressure on the primary.

By default, clients read from the primary; however, clients can specify a read preference
to send read operations to secondary nodes. Asynchronous replication to secondary nodes
means that reads from secondaries may return data that does not reflect the state of the
data on the primary. Distributed transactions that contain read operations must use read 
preference primary, all operations in a given transaction must route to the same member.

To ensure isolation and consistency, the read concern can be set to majority to indicate 
that data should only be returned to the application if it has first been replicated to 
a majority of the nodes in the replica set, and so cannot be rolled back in the event of 
the election of a new primary node.

#### References
- [Replication](https://www.mongodb.com/docs/manual/replication)
- [Multi-Document Acid Transaction](https://www.mongodb.com/blog/post/mongodb-multi-document-acid-transactions-general-availability)
- [Read Preference](https://www.mongodb.com/docs/manual/core/read-preference/)
- [Read Isolation, Consistency, and Recency](https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/)
- [Causal Consistency and Read and Write Concerns](https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/)
- [Performance Best Practices: Transactions and Read / Write Concerns](https://www.mongodb.com/blog/post/performance-best-practices-transactions-and-read-write-concerns)
- [Sharding in HA](https://www.mongodb.com/developer/products/mongodb/active-active-application-architectures/)
- [Segmenting Data by Location](https://www.mongodb.com/docs/manual/tutorial/sharding-segmenting-data-by-location/)

<hr>

## Kafka

### Message Ordering and Parallel Consumers
https://github.com/confluentinc/parallel-consumer?tab=readme-ov-file#ordering-guarantees

### Exactly Once
https://www.confluent.io/blog/simplified-robust-exactly-one-semantics-in-kafka-2-5/
https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/

### Transaction 
https://www.confluent.io/blog/transactions-apache-kafka/
https://docs.confluent.io/platform/current/kafka/authorization.html#transactional-id-resource-type-operations
https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
https://docs.spring.io/spring-kafka/reference/tips.html#ex-jdbc-sync

### KIP
Key Design Considerations of EOS and Transaction
https://cwiki.apache.org/confluence/display/KAFKA/Transactional+Messaging+in+Kafka
https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer

KIP-98 exactly-once-delivery and transactional messaging
https://cwiki.apache.org/confluence/display/KAFKA/

(Both KIP-360 and KIP-447 aims at improving the resiliency of the implementation that powers EOS and Transaction:
KIP-360 added a mechanism for producers to automatically recover when they encounter these cases and continue processing.
https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820

KIP-447 supplements KIP-360 as a fundamental improvement to the transactional semantics.
https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics

<hr>

## ZooKeeper
<hr>

## Caching
`Read-Aside Caching` `in-memory` `caching pattern`

### Cache Eviction Strategies

#### Least Recently Used (LRU)
#### Least Frequently Used (LFU)
#### Window TinyLFU (W-TinyLFU)
#### Time To Live (TTL)

#### References
- [Caching Pattern](https://blogs.vmware.com/tanzu/an-introduction-to-look-aside-vs-inline-caching-patterns)
- [Cache Eviction Policies](https://redis.io/blog/cache-eviction-strategies/)
- [LRU Cache Implementation](https://www.interviewcake.com/concept/java/lru-cache)
- [MongoDB In-Memory Database](https://www.mongodb.com/resources/basics/databases/in-memory-database)
<hr>

## Event Driven Architecture
`Eventual Consistency`
`Event-Drive Architecture` 
`SAGA`
`CQRS`
`Compensating Transaction`
`Exactly-once Delivery`
`At-least-once Delivery`
`De-duplication`
`Idempotency`

### Eventual Consistency
In a modern cloud application, the data is likely to be partitioned across data stores hosted 
at different sites, some of which could be dispersed over a wide geography. This can occur for 
a variety of reasons: to improve scalability by balancing the load across multiple computers, 
to improve response time by co-locating data close to the users and services that access it, 
or to improve availability by replicating data across different sites.

Maintaining data consistency across distributed data stores can be a significant challenge. 
The issue is that strategies such as serialization and locking only work well if all application 
instances share the same data store, and the application is designed to ensure that the locks 
are very short-lived. However, if data is partitioned or replicated across different data stores, 
locking and serializing data access to maintain consistency can become an expensive overhead 
that impacts the throughput, response time, and scalability of a system. Therefore, most modern 
distributed applications do not lock the data that they modify, and they take a rather more 
relaxed approach to consistency, known as eventual consistency.

In a system that implements strong consistency but also replicates data to remote locations, 
it may be appropriate to propagate changes to replicas outside the scope of a strongly consistent 
transaction. Some level of transient inconsistency is almost inevitable while replicas are 
updated—but the data will eventually become consistent after the synchronization between 
replicas has completed.

Eventual consistency is a pragmatic approach to data consistency. In many cases, strong consistency
is not actually required as long all the work performed by a transaction is completed or rolled
back at some point, and no updates are lost. In the eventual consistency model, data update
operations that span multiple sites can ripple through the various data stores in their own time,
without blocking concurrent application instances that access the same data.

An application may therefore see a view of a data item affected by an operation in the state
it is in while the operation is in flight, and this view may be temporarily inconsistent.
Depending on the requirements of the system, the developer might need to design applications
to detect and handle such inconsistencies, and then take steps to resolve them if necessary.
The developer must also ensure that the system does eventually become consistent. In other
words, the application is responsible for guaranteeing either that all steps in a specific
business process complete, or determining the actions to take if any of the steps fail.
How you resolve this situation in any given system is inevitably application specific.

#### References
- [Data Consistency Primer](https://learn.microsoft.com/en-us/previous-versions/msp-n-p/dn589800(v=pandp.10))

### SAGA
The Saga design pattern helps maintain data consistency in distributed systems by coordinating 
transactions across multiple services. A saga is a sequence of *local transactions* where each 
service performs its operation and initiates the next step through events or messages. If a 
step in the sequence fails, the saga executes compensating transactions to undo the completed 
steps, maintaining data consistency.

Each local transaction:
1. Completes its work atomically within a single service.
2. Updates the service's database.
3. Initiates the next transaction via an event or message.
4. If a local transaction fails, the saga executes a series of compensating transactions to reverse 
the changes made by the preceding local transactions.

There are two common saga implementation approaches, choreography and orchestration.

- Choreography
    > In choreography, services exchange events without a centralized controller. With choreography, each local transaction publishes domain events that trigger local transactions in other services

    | Pros                                                                                                                   | Cons                                                                                                                           |
    |------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
    | Good for simple workflows with few services and don't need a coordination logic.                                       | Workflow can become confusing when adding new steps. It's difficult to track which saga participants listen to which commands. |
    | No other service is required for coordination.	                                                                        | There's a risk of cyclic dependency between saga participants because they have to consume each other's commands.              |
    | Doesn't introduce a single point of failure, since the responsibilities are distributed across the saga participants.	 | Integration testing is difficult because all services must be running to simulate a transaction.                               |

- Orchestration
    > In orchestration, a centralized controller (orchestrator) handles all the transactions and tells the participants which operation to perform based on events. The orchestrator executes saga requests, stores and interprets the states of each task, and handles failure recovery with compensating transactions
    
    | Pros                                                                 | Cons                                                                                  |
    |----------------------------------------------------------------------|---------------------------------------------------------------------------------------|
    | Better suited for complex workflows or when adding new services.	    | Other design complexity requires an implementation of a coordination logic.           |
    | Avoids cyclic dependencies since the orchestrator manages the flow.	 | Introduces a point of failure because the orchestrator manages the complete workflow. |
    | Clear separation of responsibilities simplifies service logic.	      |                                                                                       |


There are many issues that you must consider if you follow this model. 
These issues are best summarized in various scenarios below: 

- Shift in design thinking
    - Adopting the Saga pattern requires a different mindset, focusing on coordinating transactions and ensuring data consistency across multiple microservices
  
- Complexity of debugging sagas
    - Debugging sagas can be complex, especially as the number of participating services grows.
  
- Irreversible local database changes
  - Data can't be rolled back because saga participants commit changes to their respective databases.
  
- Handling transient failures and idempotence
  - The system must handle transient failures effectively and ensure idempotence, where repeating the same operation doesn't alter the outcome.

- Need for monitoring and tracking sagas
  - Monitoring and tracking the workflow of a saga are essential to maintain operational oversight.

- Limitations of compensating transactions
  - Compensating transactions might not always succeed, potentially leaving the system in an inconsistent state.

#### References
- [Saga Distributed Transaction Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/saga)
- [A Saga on Sagas](https://learn.microsoft.com/en-us/previous-versions/msp-n-p/jj591569(v=pandp.10))
     
#### Retry Handling
In a distributed environment, the inability to complete an operation is often due to some 
type of temporary error (communication failure is always a possibility.) If such a failure 
occurs, an application might assume that the situation is transient and simply attempt to 
repeat the step that failed. Less transient exceptions, such as database or virtual machine 
failure, may also occur and the remedy might be similar—wait for the system to be recovered 
and then try the failing operation again. This approach could result in the same step actually 
being run twice, possibly resulting in multiple updates. It is very difficult to design a 
solution to prevent this repetition from occurring, but the application should attempt to 
render such repetition harmless.

#### Idempotency
One strategy is to design each step in an operation to be idempotent. This means that a step 
that had previously succeeded can be repeated without actually changing the state of the system. 
The steps that comprise a business operation are naturally heavily dependent on the business 
logic of your system, and the way in which you implement them will be heavily influenced by 
the structure of the data. Defining idempotent steps requires a deep, domain-specific 
understanding of your system. 

Some steps might be naturally idempotent. For example, a step that sets a particular item
to a specific value (such as “ZipCode = 11111”) can be repeated many times and the result
will always be the same.

#### De-duplication
In many cases, natural idempotency is not always possible. In a system that incorporates 
services, such as the payment system shown in the ecommerce example, it may be possible 
to implement some form of artificial idempotency. A common technique is to associate the 
message sent to the service with a unique identifier. The service can store the identifier 
for each message it receives locally, and only process a message if the identifier does 
not match that of a message it received earlier. This technique is known as de-duplication 
(the removal of duplicate messages). This strategy, exemplified by the Idempotent Receiver
pattern, depends on the service being able to store message identifiers successfully.

#### Compensation
In a distributed environment such as the cloud, implementing strong consistency is not
tolerant of the types of failure that may occur. For example, it may not be possible
to roll back a transaction and release the resources that it holds if a component
participating in the transaction has stopped responding due to a long-lasting network
outage. In this case, rather than resolve the situation through manually reconciling 
the data, you can implement compensating logic that undoes the work performed by the 
operation.

#### References
- [Compensating Transaction Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction)

### Event Sourcing
Most applications adopt the typical CRUD approach to store the latest state of the data 
in a relational database, inserting or updating data as required. 

In Event Sourcing, instead of storing just the current state of the data in a relational 
database, application code raises events that imperatively describe the action taken on 
the object. The events are generally sent to a queue where a separate process, an event 
handler, listens to the queue and persists the events in an event store. 

Event store the full series of actions taken on an object in an append-only store. The store 
acts as the system of record and can be used to materialize the domain objects. This approach 
can improve *performance*, *scalability*, and *auditability* in complex systems.

- Performance
  - On the write side, because every event is persisted in an append-only store, concurrent 
  update is avoided, thus there is no need for any synchronous lock or transaction processing.
  - On the read side, applications typically implement materialized views, read-only projections 
  of the event store that are optimized for querying.
- Scalability
  - Due to the nature of append-only write to the event store, there will be no contention 
  as the system scale out and with increased number of write requests.
- Auditability
  - Because the event store is append-only, events are immutable, providing an audit trail 
  that can be used to monitor actions taken against a data store. It can regenerate the current 
  state as materialized views or projections by replaying the events at any time, and it can 
  assist in testing and debugging the system. In addition, the requirement to use compensating 
  events to cancel changes can provide a history of changes that were reversed. This capability 
  wouldn't be the case if the model stored the current state. The list of events can also be 
  used to analyze application performance and to detect user behavior trends. Or, it can be 
  used to obtain other useful business information.

#### References
- [Event Sourcing Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)

### CQRS
Command and Query Responsibility Segregation (CQRS) is a pattern that segregates the operations 
that read data (Queries) from the operations that update data (Commands) by using separate 
interfaces. This implies that the data models used for querying and updates are different.

Separation of the read and write stores also allows each to be scaled appropriately to match 
the load. For example, read stores typically encounter a much higher load that write stores.

When the query/read model contains denormalized information (see Materialized View Pattern), 
performance is maximized when reading data for each of the views in an application or when 
querying the data in the system.

A typical approach to embracing eventual consistency is to use event sourcing in conjunction 
with CQRS so that the write model is an append-only stream of events driven by execution of 
commands. These events are used to update materialized views that act as the read model. 
For more information see Event Sourcing and CQRS.

#### References
- [CQRS Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs)
- [CQRS and Event Sourcing](https://learn.microsoft.com/en-us/previous-versions/msp-n-p/jj591577(v=pandp.10))

<hr>

## Active-Active Architecture
`High Availability`
`Fault Tolerance`
`Failover`
`Single Point of Failure`
`Multiple Regions`
`Redundancy`
`Geo Replication`
`Disaster Recovery`

### How HA works
Two important aspects of high availability are (1) a data failover system and (2) data backup.
To achieve high availability, the system has to have a way to maintain its functionality.

Typical types of failure includes:
- routine server maintenance
- software defects,
- network failure,
- hardware failure,
- software failure,
- power outages,
- anything else caused by natural disasters

#### Eliminating single points of failure
Eliminating single points of failure is key in a high-availability system. Without this safeguard, if everything was running on one server, and that server failed, the whole system would go down.

#### Implementing reliable redundancy
*Redundancy* means having backup components within the HA system. That way, if the original component fails, its "twin" can take over for it, helping to minimize downtime caused by the failure and maintain high availability.

#### Facilitating system failure detection
In the event of a component failure within the primary system, there should be clear protocols in place so that (1) the failure is obvious and documented and (2) ideally, the component can resolve the issue on its own. This is an important part of disaster recovery.

#### Achieving load balancing
Load balancing means that workloads — like network traffic — are distributed across multiple systems or servers in an efficient manner. The load balancer should be able to identify the most productive way to do this. With load balancing, no one resource or server will become overwhelmed with its workload, and high availability becomes more feasible.

#### References:
- [Facebook - Scaling Out](https://www.facebook.com/notes/10158772759002200/)
- [MongoDB - Basics of High Availability](https://www.mongodb.com/resources/basics/high-availability)
- [MongoDB - Active-Active Application Architecture](https://www.mongodb.com/developer/products/mongodb/active-active-application-architectures/)
- [Replicate Multi-Datacenter Topics Across Kafka Clusters](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/index.html)

<hr>

## P, NP, NP-Complete and NP-Hard Problems
`Computational Complexity`

#### References:
https://g.co/gemini/share/d5692e900e71
https://www.baeldung.com/cs/p-np-np-complete-np-hard
https://leimao.github.io/blog/P-VS-NP/
https://news.mit.edu/2009/explainer-pnp
https://azure.microsoft.com/en-us/resources/cloud-computing-dictionary/what-is-quantum-computing
https://ed.ted.com/lessons/the-high-stakes-race-to-make-quantum-computers-work-chiara-decaroli/digdeeper

<hr>

## Practice

### Strategy
Make assumptions to build a *MVP* with limited features, then start identifying all the potential 
problems based on the list below and design your system around it.

It's alright when you don't have a solution to address a hard problem, just make assumptions 
and inform the interviewers about the tradeoffs you're proposing rather than getting stuck. If 
your design is right the architecture will allow the system to scale, even though the performance 
may not be optimal.

1. Database
    > Always begin with looking at the data (data model), this will give you a rough idea how
      the data will be accessed (read/write), then estimate the capacity (data size) required
      for storage and computation power, the gives you insight how you can scale the data store 
      (sharding). Clarify if all data can be stored in one region or there will be requirement 
      for locality or governance.

2. Concurrency
    > If *strong consistency* is required, consider read/write requests in primary node (master), 
       otherwise distribute read requests to replicas (but beware of replication lags)

3. Business Process
    > For business process that spans across multiple services, consider using event-driven 
      architecture, beware of choosing the appropriate partition key so message ordering can 
      be guaranteed. Aim for at-least-once delivery if exactly-once cannot be guaranteed, and 
      focus on how to resolve duplicate scenario either by idempotency or de-duplication. 
      Think about how to reverse an operation without rollback database but initiate a 
      compensation.

4. Resiliency 
    > Consider what level of availability is required (active-active vs active-passive). 
      For active-active, partition the data by region such that concurrent access to data 
      in both regions are possible. For active-passive, write is limited to active region 
      while read from passive region can be outdated due to replication lag.

5. Performance
    > Consider adding cache if data can be pre-computed in a periodic basis, or if the data is 
        read-only (materialised view)

6. Audit
    > Particularly if there is any audit requirements mandating all the transactions history 
      to trace back how the current state of data is derived, or if there is a business requirement
      which detect change from database (via CDC) and aggregate the change into a separate read-only 
      materialised view (i.e. Project Helios)

### Examples
- Finance Reporting System
- Money Transfer (intra-bank) System
- Money Transfer (inter-banks) System
- Authentication and Session Management System
- Payment Gateway