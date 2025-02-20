# System Design Interview
- Requests volume drives scalability need.
- Horizontal scaling improves read performance but introduce 
challenges in data consistency for write requests.
- To achieve data consistency system needs to compromise 
write performance.

## Table of Contents
1. CAP Theorem
2. Warm-up
3. Mongo DB
4. Kafka
5. Zookeeper
6. Redis Caching
7. Event Drive Architecture
8. Active-Active Architecture
9. P, NP, NP-Complete and NP-Hard Problems

<hr>

## CAP Theorem
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

## Warm-up
`The Mentality`
`Back of the Envelope`

https://www.facebook.com/notes/10158791462547200/
https://static.googleusercontent.com/media/research.google.com/en//people/jeff/stanford-295-talk.pdf

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

### Non-Functional Requirements
- Scalability
- Consistency
- Performance
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
`Casual Consistency`

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

* duplication of data is not necessarily bad as long it results in better schema (what defines better?)

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
- [Segementing Data by Location](https://www.mongodb.com/docs/manual/tutorial/sharding-segmenting-data-by-location/)

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
`Read-Aside Caching`

https://blogs.vmware.com/tanzu/an-introduction-to-look-aside-vs-inline-caching-patterns/

<hr>

## Event Driven Architecture

https://learn.microsoft.com/en-us/previous-versions/msp-n-p/dn589800(v=pandp.10)

An event‑driven architecture has several benefits and drawbacks. It enables the implementation of transactions that span multiple services and provide eventual consistency. Another benefit is that it also enables an application to maintain materialized views. You can use events to maintain materialized views that pre‑join data owned by multiple microservices. The service that maintains the view subscribes to the relevant events and updates the view.

One drawback is that the programming model is more complex than when using ACID transactions. Each service must atomically updates the database and publishes an event, moreover you must implement compensating transactions to recover from application‑level failures; for example, you must cancel an order if the credit check fails. Also, applications must deal with inconsistent data. That is because changes made by in‑flight transactions are visible. The application can also see inconsistencies if it reads from a materialized view that is not yet updated. Another drawback is that subscribers must detect and ignore duplicate events.

CQRS
https://docs.microsoft.com/en-us/previous-versions/msp-n-p/jj591577(v=pandp.10)
https://docs.microsoft.com/en-us/previous-versions/msp-n-p/dn568103(v=pandp.10)

SAGA
https://learn.microsoft.com/en-us/azure/architecture/patterns/saga
https://learn.microsoft.com/en-us/previous-versions/msp-n-p/jj591569(v=pandp.10)

Compensation
https://learn.microsoft.com/en-us/previous-versions/msp-n-p/dn589804(v=pandp.10)

Event Sourcing
https://docs.microsoft.com/en-us/previous-versions/msp-n-p/dn589792(v=pandp.10)

<hr>

## Active-Active Architecture
`High Availability`
`Fault Tolerance`
`Failover`
`Single Point of Failure`
`Multi Regions`
`Redundancy`
`Geo Replication`
`Disaster Recovery`

### How HA works
1. Eliminating single points of failure
2. Implementing reliable redundancy
3. Facilitating system failure detection
4. Achieving load balancing

#### References:
- [Facebook - Scaling Out](https://www.facebook.com/notes/10158772759002200/)
- [MongoDB - Basics of High Availability](https://www.mongodb.com/resources/basics/high-availability)
- [MongoDB - Active-Active Application Architecture](https://www.mongodb.com/developer/products/mongodb/active-active-application-architectures/)

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