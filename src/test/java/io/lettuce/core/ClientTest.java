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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.lettuce.TestClientResources;
import io.lettuce.Wait;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

/**
 * @author Will Glozer
 * @author Mark Paluch
 */
public class ClientTest extends AbstractRedisClientTest {

    @Override
    public void openConnection() throws Exception {
        super.openConnection();
    }

    @Override
    public void closeConnection() throws Exception {
        super.closeConnection();
    }

    @Test
    public void close() {

        redis.getStatefulConnection().close();
        assertThatThrownBy(() -> redis.get(key)).isInstanceOf(RedisException.class);
    }

    @Test
    public void statefulConnectionFromSync() {
        assertThat(redis.getStatefulConnection().sync()).isSameAs(redis);
    }

    @Test
    public void statefulConnectionFromAsync() {
        RedisAsyncCommands<String, String> async = client.connect().async();
        assertThat(async.getStatefulConnection().async()).isSameAs(async);
        async.getStatefulConnection().close();
    }

    @Test
    public void statefulConnectionFromReactive() {
        RedisAsyncCommands<String, String> async = client.connect().async();
        assertThat(async.getStatefulConnection().reactive().getStatefulConnection()).isSameAs(async.getStatefulConnection());
        async.getStatefulConnection().close();
    }

    @Test
    public void timeout() {

        redis.setTimeout(0, TimeUnit.MICROSECONDS);
        assertThatThrownBy(() -> redis.eval(" os.execute(\"sleep \" .. tonumber(1))", ScriptOutputType.STATUS)).isInstanceOf(
                RedisCommandTimeoutException.class);
    }

    @Test
    public void reconnect() throws InterruptedException {

        redis.set(key, value);

        redis.quit();
        Thread.sleep(100);
        assertThat(redis.get(key)).isEqualTo(value);
        redis.quit();
        Thread.sleep(100);
        assertThat(redis.get(key)).isEqualTo(value);
        redis.quit();
        Thread.sleep(100);
        assertThat(redis.get(key)).isEqualTo(value);
    }

    @Test
    public void interrupt() {

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> redis.blpop(0, key)).isInstanceOf(RedisCommandInterruptedException.class);
    }

    @Test
    public void connectFailure() {
        RedisClient client = RedisClient.create(TestClientResources.get(), "redis://invalid");

        assertThatThrownBy(client::connect).isInstanceOf(RedisConnectionException.class).hasMessageContaining(
                "Unable to connect");

        FastShutdown.shutdown(client);
    }

    @Test
    public void connectPubSubFailure() {
        RedisClient client = RedisClient.create(TestClientResources.get(), "redis://invalid");

        assertThatThrownBy(client::connectPubSub).isInstanceOf(RedisConnectionException.class).hasMessageContaining(
                "Unable to connect");
        FastShutdown.shutdown(client);
    }

    @Test
    public void emptyClient() {

        RedisClient client = DefaultRedisClient.get();
        try {
            client.connect();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("RedisURI");
        }

        try {
            client.connect().async();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("RedisURI");
        }

        try {
            client.connect((RedisURI) null);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("RedisURI");
        }
    }

    @Test
    public void testExceptionWithCause() {
        RedisException e = new RedisException(new RuntimeException());
        assertThat(e).hasCauseExactlyInstanceOf(RuntimeException.class);
    }

    @Test
    public void reset() throws Exception {

        StatefulRedisConnection<String, String> connection = client.connect();
        RedisAsyncCommands<String, String> async = connection.async();

        connection.sync().set(key, value);
        async.reset();
        connection.sync().set(key, value);
        connection.sync().flushall();

        RedisFuture<KeyValue<String, String>> eval = async.blpop(5, key);

        Thread.sleep(500);

        assertThat(eval.isDone()).isFalse();
        assertThat(eval.isCancelled()).isFalse();

        async.reset();

        Wait.untilTrue(eval::isCancelled).waitOrTimeout();

        assertThat(eval.isCancelled()).isTrue();
        assertThat(eval.isDone()).isTrue();

        connection.close();
    }

    @Test
    public void standaloneConnectionShouldSetClientName() throws Exception {

        RedisURI redisURI = RedisURI.create(host, port);
        redisURI.setClientName("my-client");

        StatefulRedisConnection<String, String> connection = client.connect(redisURI);

        assertThat(connection.sync().clientGetname()).isEqualTo(redisURI.getClientName());

        connection.sync().quit();
        Thread.sleep(100);
        Wait.untilTrue(connection::isOpen).waitOrTimeout();

        assertThat(connection.sync().clientGetname()).isEqualTo(redisURI.getClientName());

        connection.close();
    }

    @Test
    public void pubSubConnectionShouldSetClientName() throws Exception {

        RedisURI redisURI = RedisURI.create(host, port);
        redisURI.setClientName("my-client");

        StatefulRedisConnection<String, String> connection = client.connectPubSub(redisURI);

        assertThat(connection.sync().clientGetname()).isEqualTo(redisURI.getClientName());

        connection.sync().quit();
        Thread.sleep(100);
        Wait.untilTrue(connection::isOpen).waitOrTimeout();

        assertThat(connection.sync().clientGetname()).isEqualTo(redisURI.getClientName());

        connection.close();
    }
}
