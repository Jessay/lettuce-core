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
package io.lettuce.core.cluster;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.TestClientResources;
import io.lettuce.core.FastShutdown;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSettings;
import io.lettuce.core.internal.LettuceLists;

/**
 * @author Mark Paluch
 */
public class RedisClusterClientFactoryTests {

    private final static String URI = "redis://" + TestSettings.host() + ":" + TestSettings.port();
    private final static RedisURI REDIS_URI = RedisURI.create(URI);
    private static final List<RedisURI> REDIS_URIS = LettuceLists.newList(REDIS_URI);

    @Test
    public void withStringUri() {
        FastShutdown.shutdown(RedisClusterClient.create(TestClientResources.get(), URI));
    }

    @Test
    public void withStringUriNull() {
        assertThatThrownBy(() -> RedisClusterClient.create((String) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void withUri() {
        FastShutdown.shutdown(RedisClusterClient.create(REDIS_URI));
    }

    @Test
    public void withUriUri() {
        assertThatThrownBy(() -> RedisClusterClient.create((RedisURI) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void withUriIterable() {
        FastShutdown.shutdown(RedisClusterClient.create(LettuceLists.newList(REDIS_URI)));
    }

    @Test
    public void withUriIterableNull() {
        assertThatThrownBy(() -> RedisClusterClient.create((Iterable<RedisURI>) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithStringUri() {
        FastShutdown.shutdown(RedisClusterClient.create(TestClientResources.get(), URI));
    }

    @Test
    public void clientResourcesWithStringUriNull() {
        assertThatThrownBy(() -> RedisClusterClient.create(TestClientResources.get(), (String) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesNullWithStringUri() {
        assertThatThrownBy(() -> RedisClusterClient.create(null, URI)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithUri() {
        FastShutdown.shutdown(RedisClusterClient.create(TestClientResources.get(), REDIS_URI));
    }

    @Test
    public void clientResourcesWithUriNull() {
        assertThatThrownBy(() -> RedisClusterClient.create(TestClientResources.get(), (RedisURI) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithUriUri() {
        assertThatThrownBy(() -> RedisClusterClient.create(null, REDIS_URI)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithUriIterable() {
        FastShutdown.shutdown(RedisClusterClient.create(TestClientResources.get(), LettuceLists.newList(REDIS_URI)));
    }

    @Test
    public void clientResourcesWithUriIterableNull() {
        assertThatThrownBy(() -> RedisClusterClient.create(TestClientResources.get(), (Iterable<RedisURI>) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesNullWithUriIterable() {
        assertThatThrownBy(() -> RedisClusterClient.create(null, REDIS_URIS)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientWithDifferentSslSettings() {
        assertThatThrownBy(
                () -> RedisClusterClient.create(Arrays.asList(RedisURI.create("redis://host1"),
                        RedisURI.create("redis+ssl://host1")))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientWithDifferentTlsSettings() {
        assertThatThrownBy(
                () -> RedisClusterClient.create(Arrays.asList(RedisURI.create("rediss://host1"),
                        RedisURI.create("redis+tls://host1")))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientWithDifferentVerifyPeerSettings() {
        RedisURI redisURI = RedisURI.create("rediss://host1");
        redisURI.setVerifyPeer(false);

        assertThatThrownBy(() -> RedisClusterClient.create(Arrays.asList(redisURI, RedisURI.create("rediss://host1"))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
