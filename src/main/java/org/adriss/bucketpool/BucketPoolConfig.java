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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.couchbase.client.java.Bucket;

/**
 * A simple "struct" extending the configuration of
 * {@link GenericObjectPoolConfig} to include {@link Bucket}' specific
 * configuration.
 *
 * <p>
 * This class is not thread-safe; it is only intended to be used to provide
 * attributes used when creating a pool.
 *
 * @author JC Carrillo
 * @since 0.1
 */
public class BucketPoolConfig extends GenericObjectPoolConfig {

    private String[] nodes;
    private String name;
    private boolean passivate;

    /**
     * 
     * @param name
     *            The name of the {@link Bucket}
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * 
     * @param nodes
     *            The Couchbase's nodes.
     */
    public void setNodes(final String... nodes) {
        this.nodes = nodes;
    }

    /**
     * Provides the Couchbase's nodes.
     * 
     * @return array of nodes.
     */
    public String[] getNodes() {
        return nodes;
    }

    /**
     * Provides the name of the {@link Bucket}.
     * 
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * If true, closes the {@link Bucket} when returned to the pool. This saves
     * connections but decreases performance.
     * 
     * @param passivate
     */
    public void setPassivate(boolean passivate) {
        this.passivate = passivate;
    }

    /**
     * Determines if {@link Bucket} should be closed when returned to the pool.
     * 
     * @return boolean
     */
    public boolean isPassivate() {
        return passivate;
    }
}