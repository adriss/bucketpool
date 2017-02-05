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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.datastructures.MutationOptionBuilder;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.repository.Repository;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.subdoc.LookupInBuilder;
import com.couchbase.client.java.subdoc.MutateInBuilder;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;

/**
 * Wraps a Couchbase Server {@link Bucket} to provide additional functionality..
 *
 * @author JC Carrillo
 * @since 0.1
 */
public class WrappedBucket implements Bucket {

    private final static Logger logger = LoggerFactory.getLogger(Bucket.class);

    private final WrappedCluster cluster;
    private final String name;
    private Bucket bucket;

    private final long id = System.currentTimeMillis();

    public WrappedBucket(WrappedCluster cluster, String name) {
        super();
        this.cluster = cluster;
        this.name = name;
    }

    @Override
    public AsyncBucket async() {
        return this.bucket.async();
    }

    @Override
    public ClusterFacade core() {
        return this.bucket.core();
    }

    @Override
    public CouchbaseEnvironment environment() {
        return this.bucket.environment();
    }

    @Override
    public String name() {
        return this.bucket.name();
    }

    @Override
    public JsonDocument get(String id) {
        return this.bucket.get(id);
    }

    @Override
    public JsonDocument get(String id, long timeout, TimeUnit timeUnit) {
        return this.bucket.get(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D get(D document) {
        return this.bucket.get(document);
    }

    @Override
    public <D extends Document<?>> D get(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.get(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D get(String id, Class<D> target) {
        return this.bucket.get(id, target);
    }

    @Override
    public <D extends Document<?>> D get(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
        return this.bucket.get(id, target, timeout, timeUnit);
    }

    @Override
    public boolean exists(String id) {
        return this.bucket.exists(id);
    }

    @Override
    public boolean exists(String id, long timeout, TimeUnit timeUnit) {
        return this.bucket.exists(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> boolean exists(D document) {
        return this.bucket.exists(document);
    }

    @Override
    public <D extends Document<?>> boolean exists(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.exists(document, timeout, timeUnit);
    }

    @Override
    public List<JsonDocument> getFromReplica(String id, ReplicaMode type) {
        return this.bucket.getFromReplica(id, type);
    }

    @Override
    public Iterator<JsonDocument> getFromReplica(String id) {
        return this.bucket.getFromReplica(id);
    }

    @Override
    public List<JsonDocument> getFromReplica(String id, ReplicaMode type, long timeout, TimeUnit timeUnit) {
        return this.bucket.getFromReplica(id, type, timeout, timeUnit);
    }

    @Override
    public Iterator<JsonDocument> getFromReplica(String id, long timeout, TimeUnit timeUnit) {
        return this.bucket.getFromReplica(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type) {
        return this.bucket.getFromReplica(document, type);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(D document) {
        return this.bucket.getFromReplica(document);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.getFromReplica(document, type, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.getFromReplica(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target) {
        return this.bucket.getFromReplica(id, type, target);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(String id, Class<D> target) {
        return this.bucket.getFromReplica(id, target);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.getFromReplica(id, type, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(String id, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.getFromReplica(id, target, timeout, timeUnit);
    }

    @Override
    public JsonDocument getAndLock(String id, int lockTime) {
        return this.bucket.getAndLock(id, lockTime);
    }

    @Override
    public JsonDocument getAndLock(String id, int lockTime, long timeout, TimeUnit timeUnit) {
        return this.bucket.getAndLock(id, lockTime, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndLock(D document, int lockTime) {
        return this.bucket.getAndLock(document, lockTime);
    }

    @Override
    public <D extends Document<?>> D getAndLock(D document, int lockTime, long timeout, TimeUnit timeUnit) {
        return this.bucket.getAndLock(document, lockTime, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target) {
        return this.bucket.getAndLock(id, lockTime, target);
    }

    @Override
    public <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.getAndLock(id, lockTime, target, timeout, timeUnit);
    }

    @Override
    public JsonDocument getAndTouch(String id, int expiry) {
        return this.bucket.getAndTouch(id, expiry);
    }

    @Override
    public JsonDocument getAndTouch(String id, int expiry, long timeout, TimeUnit timeUnit) {
        return this.bucket.getAndTouch(id, expiry, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(D document) {
        return this.bucket.getAndTouch(document);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.getAndTouch(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target) {
        return this.bucket.getAndTouch(id, expiry, target);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.getAndTouch(id, expiry, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document) {
        return this.bucket.insert(document);
    }

    @Override
    public <D extends Document<?>> D insert(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.insert(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.insert(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.insert(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo) {
        return this.bucket.insert(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.insert(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, ReplicateTo replicateTo) {
        return this.bucket.insert(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.insert(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document) {
        return this.bucket.upsert(document);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.upsert(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.upsert(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.upsert(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo) {
        return this.bucket.upsert(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.upsert(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo) {
        return this.bucket.upsert(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.upsert(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document) {
        return this.bucket.replace(document);
    }

    @Override
    public <D extends Document<?>> D replace(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.replace(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.replace(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.replace(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo) {
        return this.bucket.replace(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.replace(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, ReplicateTo replicateTo) {
        return this.bucket.replace(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.replace(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document) {
        return this.bucket.remove(document);
    }

    @Override
    public <D extends Document<?>> D remove(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.remove(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.remove(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo) {
        return this.bucket.remove(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, ReplicateTo replicateTo) {
        return this.bucket.remove(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id) {
        return this.bucket.remove(id);
    }

    @Override
    public JsonDocument remove(String id, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(id, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.remove(id, persistTo, replicateTo);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.remove(id, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo) {
        return this.bucket.remove(id, persistTo);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(id, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, ReplicateTo replicateTo) {
        return this.bucket.remove(id, replicateTo);
    }

    @Override
    public JsonDocument remove(String id, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(id, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, Class<D> target) {
        return this.bucket.remove(id, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(id, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target) {
        return this.bucket.remove(id, persistTo, replicateTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target,
            long timeout, TimeUnit timeUnit) {
        return this.bucket.remove(id, persistTo, replicateTo, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target) {
        return this.bucket.remove(id, persistTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.remove(id, persistTo, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target) {
        return this.bucket.remove(id, replicateTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.remove(id, replicateTo, target, timeout, timeUnit);
    }

    @Override
    public ViewResult query(ViewQuery query) {
        return this.bucket.query(query);
    }

    @Override
    public SpatialViewResult query(SpatialViewQuery query) {
        return this.bucket.query(query);
    }

    @Override
    public ViewResult query(ViewQuery query, long timeout, TimeUnit timeUnit) {
        return this.bucket.query(query, timeout, timeUnit);
    }

    @Override
    public SpatialViewResult query(SpatialViewQuery query, long timeout, TimeUnit timeUnit) {
        return this.bucket.query(query, timeout, timeUnit);
    }

    @Override
    public N1qlQueryResult query(Statement statement) {
        return this.bucket.query(statement);
    }

    @Override
    public N1qlQueryResult query(Statement statement, long timeout, TimeUnit timeUnit) {
        return this.bucket.query(statement, timeout, timeUnit);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query) {
        return this.bucket.query(query);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query, long timeout, TimeUnit timeUnit) {
        return this.bucket.query(query, timeout, timeUnit);
    }

    @Override
    public SearchQueryResult query(SearchQuery query) {
        return this.bucket.query(query);
    }

    @Override
    public SearchQueryResult query(SearchQuery query, long timeout, TimeUnit timeUnit) {
        return this.bucket.query(query, timeout, timeUnit);
    }

    @Override
    public Boolean unlock(String id, long cas) {
        return this.bucket.unlock(id, cas);
    }

    @Override
    public Boolean unlock(String id, long cas, long timeout, TimeUnit timeUnit) {
        return this.bucket.unlock(id, cas, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Boolean unlock(D document) {
        return this.bucket.unlock(document);
    }

    @Override
    public <D extends Document<?>> Boolean unlock(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.unlock(document, timeout, timeUnit);
    }

    @Override
    public Boolean touch(String id, int expiry) {
        return this.bucket.touch(id, expiry);
    }

    @Override
    public Boolean touch(String id, int expiry, long timeout, TimeUnit timeUnit) {
        return this.bucket.touch(id, expiry, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Boolean touch(D document) {
        return this.bucket.touch(document);
    }

    @Override
    public <D extends Document<?>> Boolean touch(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.touch(document, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta) {
        return this.bucket.counter(id, delta);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo) {
        return this.bucket.counter(id, delta, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial) {
        return this.bucket.counter(id, delta, initial);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo) {
        return this.bucket.counter(id, delta, initial, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, initial, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, initial, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo,
            long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry) {
        return this.bucket.counter(id, delta, initial, expiry);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo) {
        return this.bucket.counter(id, delta, initial, expiry, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, initial, expiry, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo,
            ReplicateTo replicateTo) {
        return this.bucket.counter(id, delta, initial, expiry, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, expiry, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, expiry, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, ReplicateTo replicateTo,
            long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, expiry, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo,
            ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.counter(id, delta, initial, expiry, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document) {
        return this.bucket.append(document);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo) {
        return this.bucket.append(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, ReplicateTo replicateTo) {
        return this.bucket.append(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.append(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.append(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.append(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.append(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.append(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document) {
        return this.bucket.prepend(document);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo) {
        return this.bucket.prepend(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, ReplicateTo replicateTo) {
        return this.bucket.prepend(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return this.bucket.prepend(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, long timeout, TimeUnit timeUnit) {
        return this.bucket.prepend(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.prepend(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return this.bucket.prepend(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.prepend(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public LookupInBuilder lookupIn(String docId) {
        return this.bucket.lookupIn(docId);
    }

    @Override
    public MutateInBuilder mutateIn(String docId) {
        return this.bucket.mutateIn(docId);
    }

    @Override
    public <V> boolean mapAdd(String docId, String key, V value) {
        return this.bucket.mapAdd(docId, key, value);
    }

    @Override
    public <V> boolean mapAdd(String docId, String key, V value, long timeout, TimeUnit timeUnit) {
        return this.bucket.mapAdd(docId, key, value, timeout, timeUnit);
    }

    @Override
    public <V> boolean mapAdd(String docId, String key, V value, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.mapAdd(docId, key, value, mutationOptionBuilder);
    }

    @Override
    public <V> boolean mapAdd(String docId, String key, V value, MutationOptionBuilder mutationOptionBuilder,
            long timeout, TimeUnit timeUnit) {
        return this.bucket.mapAdd(docId, key, value, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public <V> V mapGet(String docId, String key, Class<V> valueType) {
        return this.bucket.mapGet(docId, key, valueType);
    }

    @Override
    public <V> V mapGet(String docId, String key, Class<V> valueType, long timeout, TimeUnit timeUnit) {
        return this.bucket.mapGet(docId, key, valueType, timeout, timeUnit);
    }

    @Override
    public boolean mapRemove(String docId, String key) {
        return this.bucket.mapRemove(docId, key);
    }

    @Override
    public boolean mapRemove(String docId, String key, long timeout, TimeUnit timeUnit) {
        return this.bucket.mapRemove(docId, key, timeout, timeUnit);
    }

    @Override
    public boolean mapRemove(String docId, String key, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.mapRemove(docId, key, mutationOptionBuilder);
    }

    @Override
    public boolean mapRemove(String docId, String key, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.mapRemove(docId, key, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public int mapSize(String docId) {
        return this.bucket.mapSize(docId);
    }

    @Override
    public int mapSize(String docId, long timeout, TimeUnit timeUnit) {
        return this.bucket.mapSize(docId, timeout, timeUnit);
    }

    @Override
    public <E> E listGet(String docId, int index, Class<E> elementType) {
        return this.bucket.listGet(docId, index, elementType);
    }

    @Override
    public <E> E listGet(String docId, int index, Class<E> elementType, long timeout, TimeUnit timeUnit) {
        return this.bucket.listGet(docId, index, elementType, timeout, timeUnit);
    }

    @Override
    public <E> boolean listAppend(String docId, E element) {
        return this.bucket.listAppend(docId, element);
    }

    @Override
    public <E> boolean listAppend(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.listAppend(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> boolean listAppend(String docId, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.listAppend(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean listAppend(String docId, E element, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.listAppend(docId, element, mutationOptionBuilder);
    }

    @Override
    public boolean listRemove(String docId, int index) {
        return this.bucket.listRemove(docId, index);
    }

    @Override
    public boolean listRemove(String docId, int index, long timeout, TimeUnit timeUnit) {
        return this.bucket.listRemove(docId, index, timeout, timeUnit);
    }

    @Override
    public boolean listRemove(String docId, int index, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.listRemove(docId, index, mutationOptionBuilder);
    }

    @Override
    public boolean listRemove(String docId, int index, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.listRemove(docId, index, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public <E> boolean listPrepend(String docId, E element) {
        return this.bucket.listPrepend(docId, element);
    }

    @Override
    public <E> boolean listPrepend(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.listPrepend(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> boolean listPrepend(String docId, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.listPrepend(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean listPrepend(String docId, E element, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.listPrepend(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean listSet(String docId, int index, E element) {
        return this.bucket.listSet(docId, index, element);
    }

    @Override
    public <E> boolean listSet(String docId, int index, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.listSet(docId, index, element, timeout, timeUnit);
    }

    @Override
    public <E> boolean listSet(String docId, int index, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.listSet(docId, index, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean listSet(String docId, int index, E element, MutationOptionBuilder mutationOptionBuilder,
            long timeout, TimeUnit timeUnit) {
        return this.bucket.listSet(docId, index, element, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public int listSize(String docId) {
        return this.bucket.listSize(docId);
    }

    @Override
    public int listSize(String docId, long timeout, TimeUnit timeUnit) {
        return this.bucket.listSize(docId, timeout, timeUnit);
    }

    @Override
    public <E> boolean setAdd(String docId, E element) {
        return this.bucket.setAdd(docId, element);
    }

    @Override
    public <E> boolean setAdd(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.setAdd(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> boolean setAdd(String docId, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.setAdd(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean setAdd(String docId, E element, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.setAdd(docId, element, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public <E> boolean setContains(String docId, E element) {
        return this.bucket.setContains(docId, element);
    }

    @Override
    public <E> boolean setContains(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.setContains(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> E setRemove(String docId, E element) {
        return this.bucket.setRemove(docId, element);
    }

    @Override
    public <E> E setRemove(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.setRemove(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> E setRemove(String docId, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.setRemove(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> E setRemove(String docId, E element, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.setRemove(docId, element, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public int setSize(String docId) {
        return this.bucket.setSize(docId);
    }

    @Override
    public int setSize(String docId, long timeout, TimeUnit timeUnit) {
        return this.bucket.setSize(docId, timeout, timeUnit);
    }

    @Override
    public <E> boolean queuePush(String docId, E element) {
        return this.bucket.queuePush(docId, element);
    }

    @Override
    public <E> boolean queuePush(String docId, E element, long timeout, TimeUnit timeUnit) {
        return this.bucket.queuePush(docId, element, timeout, timeUnit);
    }

    @Override
    public <E> boolean queuePush(String docId, E element, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.queuePush(docId, element, mutationOptionBuilder);
    }

    @Override
    public <E> boolean queuePush(String docId, E element, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.queuePush(docId, element, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public <E> E queuePop(String docId, Class<E> elementType) {
        return this.bucket.queuePop(docId, elementType);
    }

    @Override
    public <E> E queuePop(String docId, Class<E> elementType, long timeout, TimeUnit timeUnit) {
        return this.bucket.queuePop(docId, elementType, timeout, timeUnit);
    }

    @Override
    public <E> E queuePop(String docId, Class<E> elementType, MutationOptionBuilder mutationOptionBuilder) {
        return this.bucket.queuePop(docId, elementType, mutationOptionBuilder);
    }

    @Override
    public <E> E queuePop(String docId, Class<E> elementType, MutationOptionBuilder mutationOptionBuilder, long timeout,
            TimeUnit timeUnit) {
        return this.bucket.queuePop(docId, elementType, mutationOptionBuilder, timeout, timeUnit);
    }

    @Override
    public int queueSize(String docId) {
        return this.bucket.queueSize(docId);
    }

    @Override
    public int queueSize(String docId, long timeout, TimeUnit timeUnit) {
        return this.bucket.queueSize(docId, timeout, timeUnit);
    }

    @Override
    public int invalidateQueryCache() {
        return this.bucket.invalidateQueryCache();
    }

    @Override
    public BucketManager bucketManager() {
        return this.bucket.bucketManager();
    }

    @Override
    public Repository repository() {
        return this.bucket.repository();
    }

    @Override
    public Boolean close() {
        return this.bucket.close();
    }

    @Override
    public Boolean close(long timeout, TimeUnit timeUnit) {
        return this.bucket.close(timeout, timeUnit);
    }

    @Override
    public boolean isClosed() {
        return this.bucket == null || this.bucket.isClosed();
    }

    public void open() {
        logger.info("Opening {} bucket", this);
        this.bucket = this.cluster.openBucket(this.name);
        logger.info("Opened {} bucket", this);
    }

    @Override
    public String toString() {
        return this.cluster + "-" + this.name + "-" + Long.toString(this.id);
    }

    public WrappedCluster getCluster() {
        return this.cluster;
    }
}