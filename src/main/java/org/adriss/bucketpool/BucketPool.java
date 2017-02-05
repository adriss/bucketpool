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

import java.util.stream.IntStream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

/**
 * <code>BucketPool</code> is an implementation of {@link GenericObjectPool} to
 * provide a robust pooling functionality for {@link Bucket} objects.
 * <p>
 * This class is intended to be thread-safe.
 * </p>
 *
 * @author JC Carrillo
 * @since 0.1
 */
public class BucketPool extends GenericObjectPool<Bucket> {

    private final static Logger logger = LoggerFactory.getLogger(BucketPool.class);

    /**
     * @param config
     *            The configuration to use for this pool instance. The
     *            configuration is used by value. Subsequent changes to the
     *            configuration object will not be reflected in the pool.
     * @param abandonedConfig
     *            Configuration for abandoned object identification and removal.
     *            The configuration is used by value.
     * @throws Exception
     */
    public BucketPool(BucketPoolConfig config, BucketPoolAbandonedConfig abandonedConfig) throws Exception {
        super(new BucketFactory(config), config, abandonedConfig);
        logger.info("Initializing {} buckets", config.getMinIdle());
        Bucket bucket[] = new Bucket[config.getMinIdle()];
        IntStream.range(0, config.getMinIdle()).forEach(x -> {
            try {
                bucket[x] = borrowObject();
                logger.debug("Bucket [{}] borrowed", bucket[x]);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        });
        IntStream.range(0, config.getMinIdle()).forEach(x -> {
            returnObject(bucket[x]);
            logger.debug("Bucket [{}] returned", bucket[x]);
        });
        logger.info("Initialized {} buckets", config.getMinIdle());
    }

    /**
     * Create a new <code>BucketPool</code> using a specific configuration.
     *
     * @param config
     *            The configuration to use for this pool instance. The
     *            configuration is used by value. Subsequent changes to the
     *            configuration object will not be reflected in the pool.
     * @throws Exception
     */
    public BucketPool(BucketPoolConfig config) throws Exception {
        this(config, null);
    }
}