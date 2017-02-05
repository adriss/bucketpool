package org.adriss.bucketpool.metrics;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.adriss.bucketpool.BucketPool;
import org.adriss.bucketpool.BucketPoolConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

@RunWith(BlockJUnit4ClassRunner.class)
public class ExampleBucketPoolTest {

    private BucketPool bucketPool;

    @Before
    public void before() throws Exception {
        BucketPoolConfig config = new BucketPoolConfig();
        config.setMaxTotal(4);
        config.setMaxIdle(3);
        config.setMinIdle(2);
        config.setPassivate(false);
        config.setName("default");
        config.setNodes("localhost");
        this.bucketPool = new BucketPool(config);
    }

    @Test
    public void test() {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        long startTime = System.nanoTime();
        Set<Future<Boolean>> futures = new HashSet<>();
        final int amount = 10000;
        IntStream.range(0, amount).forEach(nbr -> {
            futures.add(executor.submit((Callable<Boolean>) () -> {
                Bucket bucket = null;
                try {
                    bucket = this.bucketPool.borrowObject();
                    bucket.upsert(JsonDocument.create("u:example", JsonObject.create().put("name", "myDoc")));
                    return true;
                } finally {
                    this.bucketPool.returnObject(bucket);
                }
            }));
        });
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
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
        Assert.assertTrue((System.nanoTime() - startTime) / 1000000 < 1500);
    }
}