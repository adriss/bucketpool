/*
 * Copyright (C) 2017 Adriss, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.adriss.bucketpool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * A {@link Bucket} implementation of <code>BasePooledObjectFactory</code>.
 * <p>
 * 
 * @author JC Carrillo
 * @since 0.1
 */
public class BucketFactory extends BasePooledObjectFactory<Bucket> {

    private final static Logger logger = LoggerFactory.getLogger(BucketFactory.class);

    private String name;
    private String[] nodes;
    private boolean passivate;
    private Queue<WrappedCluster> unusedClusters = new ConcurrentLinkedQueue<>();
    private DefaultCouchbaseEnvironment environment;

    /**
     * @param config
     *            The configuration to use for this pool instance. The
     *            configuration is used by value. Subsequent changes to the
     *            configuration object will not be reflected in the pool.
     */
    public BucketFactory(BucketPoolConfig config) {
        super();
        this.nodes = config.getNodes();
        if (this.nodes == null) {
            this.nodes = new String[] { "localhost" };
        }
        this.name = config.getName();
        if (this.name == null) {
            this.name = "default";
        }
        this.passivate = config.isPassivate();
        this.environment = DefaultCouchbaseEnvironment.create();
        initClusters(config.getMaxTotal());
    }

    private void initClusters(int clusterAmount) {
        logger.info("Initializing {} clusters", clusterAmount);
        IntStream.range(0, clusterAmount).forEach(nbr -> {
            WrappedCluster cluster = new WrappedCluster(CouchbaseCluster.create(this.environment, this.nodes),
                    this.name);
            logger.debug("Created [{}] cluster", cluster);
            this.unusedClusters.offer(cluster);
        });
        logger.info("Initialized {} clusters", clusterAmount);
    }

    @Override
    public Bucket create() throws Exception {
        synchronized (this.unusedClusters) {
            WrappedCluster cluster = this.unusedClusters.poll();
            if (cluster == null) {
                cluster = new WrappedCluster(CouchbaseCluster.create(this.nodes), this.name);
            }
            WrappedBucket bucket = new WrappedBucket(cluster, this.name);
            bucket.open();
            return bucket;
        }
    }

    @Override
    public void destroyObject(PooledObject<Bucket> p) throws Exception {
        WrappedBucket bucket = (WrappedBucket) p.getObject();
        logger.debug("Destroyed [{}] bucket", bucket);
        WrappedCluster cluster = bucket.getCluster();
        if (bucket.close()) {
            this.unusedClusters.add(cluster);
        }
    }

    @Override
    public boolean validateObject(PooledObject<Bucket> p) {
        return !p.getObject().isClosed();
    }

    @Override
    public PooledObject<Bucket> wrap(Bucket bucket) {
        return new DefaultPooledObject<Bucket>(bucket);
    }

    @Override
    public void activateObject(PooledObject<Bucket> p) throws Exception {
        WrappedBucket bucket = (WrappedBucket) p.getObject();
        if (bucket.isClosed()) {
            bucket.open();
        }
    }

    @Override
    public void passivateObject(PooledObject<Bucket> p) throws Exception {
        if (this.passivate) {
            p.getObject().close();
        }
    }
}