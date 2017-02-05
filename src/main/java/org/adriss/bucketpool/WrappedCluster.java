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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.auth.Authenticator;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.transcoder.Transcoder;

/**
 * Wraps a Couchbase Server {@link Cluster} to provide additional functionality.
 *
 *
 * @author JC Carrillo
 * @since 0.1
 */
public class WrappedCluster implements Cluster {

    private String name;
    private Cluster cluster;
    private final long id = System.currentTimeMillis();

    public WrappedCluster(Cluster cluster, String name) {
        super();
        this.cluster = cluster;
        this.name = name;
    }

    @Override
    public Bucket openBucket() {
        return this.cluster.openBucket();
    }

    @Override
    public Bucket openBucket(long timeout, TimeUnit timeUnit) {
        return this.cluster.openBucket(timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name) {
        return this.cluster.openBucket(name);
    }

    @Override
    public Bucket openBucket(String name, long timeout, TimeUnit timeUnit) {
        return this.cluster.openBucket(name, timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name, String password) {
        return this.cluster.openBucket(name, password);
    }

    @Override
    public Bucket openBucket(String name, String password, long timeout, TimeUnit timeUnit) {
        return this.cluster.openBucket(name, password, timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders) {
        return this.cluster.openBucket(name, password, transcoders);
    }

    @Override
    public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders,
            long timeout, TimeUnit timeUnit) {
        return this.cluster.openBucket(name, password, transcoders, timeout, timeUnit);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query) {
        return this.cluster.query(query);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query, long timeout, TimeUnit timeUnit) {
        return this.cluster.query(query, timeout, timeUnit);
    }

    @Override
    public ClusterManager clusterManager(String username, String password) {
        return this.cluster.clusterManager(username, password);
    }

    @Override
    public ClusterManager clusterManager() {
        return this.cluster.clusterManager();
    }

    @Override
    public Boolean disconnect() {
        return this.cluster.disconnect();
    }

    @Override
    public Boolean disconnect(long timeout, TimeUnit timeUnit) {
        return this.cluster.disconnect(timeout, timeUnit);
    }

    @Override
    public ClusterFacade core() {
        return this.cluster.core();
    }

    @Override
    public Cluster authenticate(Authenticator auth) {
        return this.cluster.authenticate(auth);
    }

    @Override
    public String toString() {
        return this.name + "-" + Long.toString(this.id);
    }
}