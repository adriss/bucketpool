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
    private float NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private int EXPECTED_THREAD_EXECUTION_TIME_IN_MILLS = 1000;

    @Before
    public void before() throws Exception {
        // get the runtime object associated with the current Java application
        Runtime runtime = Runtime.getRuntime();
        // get the number of processors available to the Java virtual machine

        BucketPoolConfig config = new BucketPoolConfig();
        config.setMaxTotal((int) NUMBER_OF_PROCESSORS);
        config.setMaxIdle((int) Math.ceil(NUMBER_OF_PROCESSORS / 2));
        config.setMinIdle((int) Math.ceil(NUMBER_OF_PROCESSORS / 4));
        config.setPassivate(false);
        config.setName("default");
        config.setNodes("localhost");
        this.bucketPool = new BucketPool(config);
    }

    @Test
    public void upsert() {
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
        Assert.assertTrue("Execution time was more than expected", ((System.nanoTime() - startTime)
                / 1000000) < (EXPECTED_THREAD_EXECUTION_TIME_IN_MILLS * NUMBER_OF_PROCESSORS) / 2);
    }
}