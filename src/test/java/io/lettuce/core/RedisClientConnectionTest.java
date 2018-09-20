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

import static io.lettuce.core.RedisURI.Builder.redis;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

/**
 * @author Mark Paluch
 * @author Jongyeol Choi
 */
public class RedisClientConnectionTest extends AbstractRedisClientTest {

    public static final Duration EXPECTED_TIMEOUT = Duration.ofMillis(500);

    @BeforeEach
    public void before() {
        client.setDefaultTimeout(EXPECTED_TIMEOUT);
    }

    /*
     * Standalone/Stateful
     */
    @Test
    public void connectClientUri() {

        StatefulRedisConnection<String, String> connection = client.connect();
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectCodecClientUri() {
        StatefulRedisConnection<String, String> connection = client.connect(UTF8);
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        StatefulRedisConnection<String, String> connection = client.connect(redisURI);
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connect(new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connect(invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectCodecOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        StatefulRedisConnection<String, String> connection = client.connect(UTF8, redisURI);
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectAsyncCodecOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        ConnectionFuture<StatefulRedisConnection<String, String>> future = client.connectAsync(UTF8, redisURI);
        StatefulRedisConnection<String, String> connection = future.join();
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectCodecMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connect(UTF8, new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectcodecSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connect(UTF8, invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Disabled("Non-deterministic behavior. Can cause a deadlock")
    public void shutdownSyncInRedisFutureTest() throws Exception {

        RedisClient redisClient = RedisClient.create();
        StatefulRedisConnection<String, String> connection = redisClient.connect(redis(host, port).build());

        CompletableFuture<String> f = connection.async().get("key1").whenComplete((result, e) -> {
            connection.close();
            redisClient.shutdown(0, 0, SECONDS); // deadlock expected.
            }).toCompletableFuture();

        assertThatThrownBy(() -> f.get(1, SECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void shutdownAsyncInRedisFutureTest() throws Exception {

        RedisClient redisClient = RedisClient.create();
        StatefulRedisConnection<String, String> connection = redisClient.connect(redis(host, port).build());
        CompletableFuture<Void> f = connection.async().get("key1").thenCompose(result -> {
            connection.close();
            return redisClient.shutdownAsync(0, 0, SECONDS);
        }).toCompletableFuture();

        f.get(5, SECONDS);
    }

    /*
     * Standalone/PubSub Stateful
     */
    @Test
    public void connectPubSubClientUri() {
        StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub();
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectPubSubCodecClientUri() {
        StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub(UTF8);
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectPubSubOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub(redisURI);
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectPubSubMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectPubSub(new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectPubSubSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectPubSub(invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectPubSubCodecOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub(UTF8, redisURI);
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectPubSubAsync() {
        RedisURI redisURI = redis(host, port).build();
        ConnectionFuture<StatefulRedisPubSubConnection<String, String>> future = client.connectPubSubAsync(
UTF8, redisURI);
        StatefulRedisPubSubConnection<String, String> connection = future.join();
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectPubSubCodecMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectPubSub(UTF8, new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectPubSubCodecSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectPubSub(UTF8, invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    /*
     * Sentinel Stateful
     */
    @Test
    public void connectSentinelClientUri() {
        StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel();
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectSentinelCodecClientUri() {
        StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel(UTF8);
        assertThat(connection.getTimeout()).isEqualTo(EXPECTED_TIMEOUT);
        connection.close();
    }

    @Test
    public void connectSentinelAndMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectSentinel(new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectSentinelSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectSentinel(invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectSentinelOwnUri() {
        RedisURI redisURI = redis(host, port).build();
        StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel(redisURI);
        assertThat(connection.getTimeout()).isEqualTo(Duration.ofMinutes(1));
        connection.close();
    }

    @Test
    public void connectSentinelCodecOwnUri() {

        RedisURI redisURI = redis(host, port).build();
        StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel(UTF8, redisURI);
        assertThat(connection.getTimeout()).isEqualTo(redisURI.getTimeout());
        connection.close();
    }

    @Test
    public void connectSentinelCodecMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectSentinel(UTF8, new RedisURI())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void connectSentinelCodecSentinelMissingHostAndSocketUri() {
        assertThatThrownBy(() -> client.connectSentinel(UTF8, invalidSentinel())).isInstanceOf(IllegalArgumentException.class);
    }

    private static RedisURI invalidSentinel() {

        RedisURI redisURI = new RedisURI();
        redisURI.getSentinels().add(new RedisURI());

        return redisURI;
    }
}
