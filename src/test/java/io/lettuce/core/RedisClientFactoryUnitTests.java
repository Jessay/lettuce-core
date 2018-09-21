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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.lettuce.test.resource.TestClientResources;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.test.resource.FastShutdown;
import io.lettuce.test.settings.TestSettings;

/**
 * @author Mark Paluch
 */
public class RedisClientFactoryUnitTests {

    private final static String URI = "redis://" + TestSettings.host() + ":" + TestSettings.port();
    private final static RedisURI REDIS_URI = RedisURI.create(URI);

    @Test
    public void plain() {
        FastShutdown.shutdown(RedisClient.create());
    }

    @Test
    public void withStringUri() {
        FastShutdown.shutdown(RedisClient.create(URI));
    }

    @Test
    public void withStringUriNull() {
        assertThatThrownBy(() -> RedisClient.create((String) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void withUri() {
        FastShutdown.shutdown(RedisClient.create(REDIS_URI));
    }

    @Test
    public void withUriNull() {
        assertThatThrownBy(() -> RedisClient.create((RedisURI) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientResources() {
        FastShutdown.shutdown(RedisClient.create(TestClientResources.get()));
    }

    @Test
    public void clientResourcesNull() {
        assertThatThrownBy(() -> RedisClient.create((ClientResources) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithStringUri() {
        FastShutdown.shutdown(RedisClient.create(TestClientResources.get(), URI));
    }

    @Test
    public void clientResourcesWithStringUriNull() {
        assertThatThrownBy(() -> RedisClient.create(TestClientResources.get(), (String) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesNullWithStringUri() {
        assertThatThrownBy(() -> RedisClient.create(null, URI)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesWithUri() {
        FastShutdown.shutdown(RedisClient.create(TestClientResources.get(),  REDIS_URI));
    }

    @Test
    public void clientResourcesWithUriNull() {
        assertThatThrownBy(() -> RedisClient.create(TestClientResources.get(), (RedisURI) null)).isInstanceOf(
                IllegalArgumentException.class);
    }

    @Test
    public void clientResourcesNullWithUri() {
        assertThatThrownBy(() -> RedisClient.create(null, REDIS_URI)).isInstanceOf(IllegalArgumentException.class);
    }
}
