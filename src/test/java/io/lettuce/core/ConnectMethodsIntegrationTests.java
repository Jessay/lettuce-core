/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core;

import javax.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.AsyncNodeSelection;
import io.lettuce.test.LettuceExtension;

/**
 * @author Mark Paluch
 */
@ExtendWith(LettuceExtension.class)
public class ConnectMethodsIntegrationTests {

    private final RedisClient redisClient;
    private final RedisClusterClient clusterClient;

    @Inject
    public ConnectMethodsIntegrationTests(RedisClient redisClient, RedisClusterClient clusterClient) {
        this.redisClient = redisClient;
        this.clusterClient = clusterClient;
    }

    // Standalone
    @Test
    public void standaloneSync() {
        redisClient.connect().close();
    }

    @Test
    public void standaloneAsync() {
        redisClient.connect().async().getStatefulConnection().close();
    }

    @Test
    public void standaloneReactive() {
        redisClient.connect().reactive().getStatefulConnection().close();
    }

    @Test
    public void standaloneStateful() {
        redisClient.connect().close();
    }

    // PubSub
    @Test
    public void pubsubSync() {
        redisClient.connectPubSub().close();
    }

    @Test
    public void pubsubAsync() {
        redisClient.connectPubSub().close();
    }

    @Test
    public void pubsubReactive() {
        redisClient.connectPubSub().close();
    }

    @Test
    public void pubsubStateful() {
        redisClient.connectPubSub().close();
    }

    // Sentinel
    @Test
    public void sentinelSync() {
        redisClient.connectSentinel().sync().getStatefulConnection().close();
    }

    @Test
    public void sentinelAsync() {
        redisClient.connectSentinel().async().getStatefulConnection().close();
    }

    @Test
    public void sentinelReactive() {
        redisClient.connectSentinel().reactive().getStatefulConnection().close();
    }

    @Test
    public void sentinelStateful() {
        redisClient.connectSentinel().close();
    }

    // Cluster
    @Test
    public void clusterSync() {
        clusterClient.connect().sync().getStatefulConnection().close();
    }

    @Test
    public void clusterAsync() {
        clusterClient.connect().async().getStatefulConnection().close();
    }

    @Test
    public void clusterReactive() {
        clusterClient.connect().reactive().getStatefulConnection().close();
    }

    @Test
    public void clusterStateful() {
        clusterClient.connect().close();
    }

    @Test
    public void clusterPubSubSync() {
        clusterClient.connectPubSub().sync().getStatefulConnection().close();
    }

    @Test
    public void clusterPubSubAsync() {
        clusterClient.connectPubSub().async().getStatefulConnection().close();
    }

    @Test
    public void clusterPubSubReactive() {
        clusterClient.connectPubSub().reactive().getStatefulConnection().close();
    }

    @Test
    public void clusterPubSubStateful() {
        clusterClient.connectPubSub().close();
    }

    // Advanced Cluster
    @Test
    public void advancedClusterSync() {
        StatefulRedisClusterConnection<String, String> statefulConnection = clusterClient.connect();
        RedisURI uri = clusterClient.getPartitions().getPartition(0).getUri();
        statefulConnection.getConnection(uri.getHost(), uri.getPort()).sync();
        statefulConnection.close();
    }

    @Test
    public void advancedClusterAsync() {
        StatefulRedisClusterConnection<String, String> statefulConnection = clusterClient.connect();
        RedisURI uri = clusterClient.getPartitions().getPartition(0).getUri();
        statefulConnection.getConnection(uri.getHost(), uri.getPort()).sync();
        statefulConnection.close();
    }

    @Test
    public void advancedClusterReactive() {
        StatefulRedisClusterConnection<String, String> statefulConnection = clusterClient.connect();
        RedisURI uri = clusterClient.getPartitions().getPartition(0).getUri();
        statefulConnection.getConnection(uri.getHost(), uri.getPort()).reactive();
        statefulConnection.close();
    }

    @Test
    public void advancedClusterStateful() {
        clusterClient.connect().close();
    }

    // Cluster node selection
    @Test
    public void nodeSelectionClusterAsync() {
        StatefulRedisClusterConnection<String, String> statefulConnection = clusterClient.connect();
        AsyncNodeSelection<String, String> masters = statefulConnection.async().masters();
        statefulConnection.close();
    }

}
