# System Design Interview

### Table of Contents
1. CAP Theorem
2. Warm-up
3. Collecting Requirements
4. Event Drive Architecture
5. Active-Active Architecture

<hr>

### CAP Theorem
- `C` 
  - The system has only one node and therefore data consistency (C) is not a concern, but it doesn’t provide high availability (A) over networked nodes (P) 
- `AP`
  - The system has multiple nodes across the network (P) to provides high availability (A), but then data consistency across the nodes cannot be achieved (C), as data replication over the networked node is susceptible to delay and failure, consistency cannot be guaranteed. Data will only be eventually consistent across all the nodes when network is resumed.
- `CP`
  - The system has multiple nodes across the network (P), however in favour of data consistency (C), it needs to stop accepting write requests (give up A) before data synchronisation is completed across all the nodes
- `AC`
  - Theoretically doesn’t exist because high availability (A) requires partition tolerance (P), but once networked nodes are introduced, consistency cannot be guaranteed unless we give-up availability, or we have to tradeoff consistency for high availability (see AP and CP above).
- `ACP`
  - The system has multiple nodes across the network (P) for the sake of availability (A), however in favour of data consistency (C), it can only accept write on a single node (A is limited) AND forced to use a synchronous replication to all the networked nodes (P), with a tradeoff in performance (write latency). Synchronous replication must be atomic (rollback all everything if replication timeout) to achieve data consistency across all replica.

### Warm-up
`The Mentality`
`Back of the Envelope`

[Facebook - The full stack part 1](https://www.facebook.com/notes/10158791462547200/)

### Collecting Requirements
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

<hr>

### MongoDB
`NoSQL`
`Schema Design`
`Sharding`
`ReplicaSet`
`Transaction`
`Read Write Concern`
https://www.mongodb.com/resources/basics/databases/nosql-explained
https://www.mongodb.com/developer/products/mongodb/mongodb-schema-design-best-practices/
https://www.mongodb.com/resources/products/capabilities/sharding
https://www.mongodb.com/docs/manual/core/sharding-choose-a-shard-key/
https://www.mongodb.com/docs/manual/replication/
https://www.mongodb.com/blog/post/mongodb-multi-document-acid-transactions-general-availability
https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/
https://www.mongodb.com/blog/post/performance-best-practices-transactions-and-read-write-concerns
https://www.mongodb.com/developer/products/mongodb/active-active-application-architectures/

### Kafka


### ZooKeeper

### Caching
`Read-Aside Caching`
https://blogs.vmware.com/tanzu/an-introduction-to-look-aside-vs-inline-caching-patterns/

### Event Driven Architecture
https://learn.microsoft.com/en-us/previous-versions/msp-n-p/dn589800(v=pandp.10)


### Active-Active Architecture
`Scaling Out`
`Multi Regions`
`Disaster Recovery`

[Facebook - Scaling Out](https://www.facebook.com/notes/10158772759002200/)
[MongoDB - Active-Active Application Architecture](https://www.mongodb.com/developer/products/mongodb/active-active-application-architectures/)


### P, NP, NP-Complete and NP-Hard Problems
https://g.co/gemini/share/d5692e900e71
https://www.baeldung.com/cs/p-np-np-complete-np-hard
https://leimao.github.io/blog/P-VS-NP/
https://news.mit.edu/2009/explainer-pnp
https://azure.microsoft.com/en-us/resources/cloud-computing-dictionary/what-is-quantum-computing
https://ed.ted.com/lessons/the-high-stakes-race-to-make-quantum-computers-work-chiara-decaroli/digdeeper
