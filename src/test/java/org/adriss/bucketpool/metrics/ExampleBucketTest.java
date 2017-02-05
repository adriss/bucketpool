package org.adriss.bucketpool.metrics;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

@RunWith(BlockJUnit4ClassRunner.class)
public class ExampleBucketTest {

    @Test
    public void test() {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        Cluster cluster = CouchbaseCluster.create("localhost");
        Bucket bucket = cluster.openBucket("default");
        Set<Future<Boolean>> futures = new HashSet<>();
        long startTime = System.nanoTime();
        final int amount = 10000;
        IntStream.range(0, amount).forEach(nbr -> {
            futures.add(executor.submit((Callable<Boolean>) () -> {
                bucket.upsert(JsonDocument.create("u:example", JsonObject.create().put("name", "myDoc")));
                return true;
            }));
        });
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {
        }
        int count = (int) futures.stream().filter((future) -> {
            try {
                return future.get().equals(true);
            } catch (Throwable e1) {
                return false;
            }
        }).count();
        System.out.println("Total execution time in millis: " + (System.nanoTime() - startTime) / 1000000);
        Assert.assertEquals(amount, count);
    }
}