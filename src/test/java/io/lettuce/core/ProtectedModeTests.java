/*
 * Copyright 2017-2018 the original author or authors.
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.lettuce.TestClientResources;
import io.lettuce.Wait;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.server.MockTcpServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author Mark Paluch
 */
public class ProtectedModeTests {

    private static MockTcpServer server;
    private static RedisClient client;

    @BeforeAll
    public static void beforeClass() throws Exception {

        server = new MockTcpServer();

        server.addHandler(() -> {
            return new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) throws Exception {

                    String message = getMessage();
                    ByteBuf buffer = ctx.alloc().buffer(message.length() + 3);
                    buffer.writeCharSequence("-", StandardCharsets.US_ASCII);
                    buffer.writeCharSequence(message, StandardCharsets.US_ASCII);
                    buffer.writeByte('\r').writeByte('\n');

                    ctx.writeAndFlush(buffer).addListener(future -> {
                        ctx.close();
                    });
                }
            };
        });

        server.initialize(TestSettings.nonexistentPort());

        client = RedisClient.create(TestClientResources.get(),
                RedisURI.create(TestSettings.host(), TestSettings.nonexistentPort()));
    }

    @AfterAll
    public static void afterClass() {

        server.shutdown();
        FastShutdown.shutdown(client);
    }

    @BeforeEach
    public void before() {
        client.setOptions(ClientOptions.create());
    }

    @Test
    public void regularClientFailsOnFirstCommand() {

        try (StatefulRedisConnection<String, String> connect = client.connect()) {

            connect.sync().ping();
        } catch (RedisException e) {
            if (e.getCause() instanceof IOException) {
                assertThat(e).hasCauseInstanceOf(IOException.class);
            } else {
                assertThat(e.getCause()).hasMessageContaining("DENIED");
            }
        }
    }

    @Test
    public void regularClientFailsOnFirstCommandWithDelay() {

        try (StatefulRedisConnection<String, String> connect = client.connect()) {

            Wait.untilEquals(false, connect::isOpen).waitOrTimeout();

            connect.sync().ping();
        } catch (RedisException e) {
            if (e.getCause() instanceof IOException) {
                assertThat(e).hasCauseInstanceOf(IOException.class);
            } else {
                assertThat(e.getCause()).hasMessageContaining("DENIED");
            }
        }
    }

    @Test
    public void pingBeforeConnectFailsOnPing() {

        client.setOptions(ClientOptions.builder().pingBeforeActivateConnection(true).build());
        assertThatThrownBy(() -> client.connect()).isInstanceOf(RedisConnectionException.class).hasCauseInstanceOf(
                RedisConnectionException.class);
    }

    private static String getMessage() {

        return "DENIED Redis is running in protected mode because protected mode is enabled, no bind address was specified, "
                + "no authentication password is requested to clients. In this mode connections are only accepted from the "
                + "loopback interface. If you want to connect from external computers to Redis you may adopt one of the "
                + "following solutions: 1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' "
                + "from the loopback interface by connecting to Redis from the same host the server is running, however "
                + "MAKE SURE Redis is not publicly accessible from internet if you do so. Use CONFIG REWRITE to make this "
                + "change permanent. 2) Alternatively you can just disable the protected mode by editing the Redis "
                + "configuration file, and setting the protected mode option to 'no', and then restarting the server. "
                + "3) If you started the server manually just for testing, restart it with the '--protected-mode no' option. "
                + "4) Setup a bind address or an authentication password. NOTE: You only need to do one of the above "
                + "things in order for the server to start accepting connections from the outside.";

    }
}
