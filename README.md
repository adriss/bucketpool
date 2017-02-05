# bucketpool
Couchbase's Bucket Pool implementation of Apache Commons Pool v2.
----------------------------------------------------
Couchbase's Java SDK has a thread safe implementation that allows multiple threads to operate on a single Bucket. The snippet below shows how multiple threads can work simultaneously.

The exercise is to create a thread pool to update a range of Couchbase documents.
Consider the values below:
```java
ExecutorService executor = Executors.newFixedThreadPool(threadPool);
int maxUpdates = 10000;
```
Executing a vanilla Couchbase implementation (shown below),
```java
Cluster cluster = CouchbaseCluster.create();
Bucket bucket = cluster.openBucket();
IntStream.range(0, maxUpdates).forEach(nbr -> {
    executor.submit(() -> {
        bucket.upsert(JsonDocument.create("u:example", JsonObject.create().put("name", "myDoc")));
    });
});
executor.shutdown();
while (!executor.isTerminated()) {
}
```
yields an execution time of 2750 to 2950 milliseconds.

The code above creates a pool of 100 threads to execute 10,000 bucket updates. Since the `bucket` is thread-safe, every call succeeds by  all sharing the `bucket`'s resources.

Utilizing the `bucketpool` as shown below, will yield an execution time of 650 to 780 milliseconds. The throughput comes at a cost, which is a higher number of connections but so is any database connection pool.
```java
BucketPoolConfig config = new BucketPoolConfig();
config.setMaxTotal(4); // starts 4 CouchbaseClusters. each cluster will create a bucket
config.setMaxIdle(3); // will allow only up-to 3 idle buckets to be in the pool
config.setMinIdle(2); // will keep up-to 2 idle buckets in the pool
config.setPassivate(false); // doesn't close the buckets when returned to the pool
bucketPool = new BucketPool(config); // creates and initializes the pool

Cluster cluster = CouchbaseCluster.create();
Bucket bucket = cluster.openBucket();
IntStream.range(0, maxUpdates).forEach(nbr -> {
    executor.submit(() -> {
        Bucket bucket = null;
        try {
            bucket = this.bucketPool.borrowObject(); // borrow bucket from pool
            bucket.upsert(JsonDocument.create("u:example", JsonObject.create().put("name", "myDoc")));
        } finally {
            bucketPool.returnObject(bucket); // always return bucket back to the pool
        }
    });
});
executor.shutdown();
while (!executor.isTerminated()) {
}
```
The Couchbase SDK prevents us from creating more than one of the "same" `Bucket` from one `Cluster`, therefore we need to create one `Cluster` per `Bucket`. All `Cluster`s use the same `com.couchbase.client.java.env.DefaultCouchbaseEnvironment` as advised in https://developer.couchbase.com/documentation/server/4.0/sdks/java-2.2/managing-connections.html.


Note: all metrics were using a 2.8 GHz (Quad-core) Intel Core i7, 16 GB 1600 MHz DDR3 MacBook Pro.
