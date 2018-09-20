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
package io.lettuce.core.protocol;

import static io.lettuce.core.protocol.RedisStateMachine.State;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import io.lettuce.core.output.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author Will Glozer
 * @author Mark Paluch
 */
public class StateMachineTest {
    protected RedisCodec<String, String> codec = new Utf8StringCodec();
    protected Charset charset = Charset.forName("UTF-8");
    protected CommandOutput<String, String, String> output;
    protected RedisStateMachine rsm;

    @BeforeAll
    public static void beforeClass() {

        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(RedisStateMachine.class.getName());
        loggerConfig.setLevel(Level.ALL);
    }

    @AfterAll
    public static void afterClass() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(RedisStateMachine.class.getName());
        loggerConfig.setLevel(null);
    }

    @BeforeEach
    public final void createStateMachine() {
        output = new StatusOutput<String, String>(codec);
        rsm = new RedisStateMachine();
    }

    @Test
    public void single() {
        assertThat(rsm.decode(buffer("+OK\r\n"), output)).isTrue();
        assertThat(output.get()).isEqualTo("OK");
    }

    @Test
    public void error() {
        assertThat(rsm.decode(buffer("-ERR\r\n"), output)).isTrue();
        assertThat(output.getError()).isEqualTo("ERR");
    }

    @Test
    public void errorWithoutLineBreak() {
        assertThat(rsm.decode(buffer("-ERR"), output)).isFalse();
        assertThat(rsm.decode(buffer("\r\n"), output)).isTrue();
        assertThat(output.getError()).isEqualTo("");
    }

    @Test
    public void integer() {
        CommandOutput<String, String, Long> output = new IntegerOutput<String, String>(codec);
        assertThat(rsm.decode(buffer(":1\r\n"), output)).isTrue();
        assertThat((long) output.get()).isEqualTo(1);
    }

    @Test
    public void bulk() {
        CommandOutput<String, String, String> output = new ValueOutput<String, String>(codec);
        assertThat(rsm.decode(buffer("$-1\r\n"), output)).isTrue();
        assertThat(output.get()).isNull();
        assertThat(rsm.decode(buffer("$3\r\nfoo\r\n"), output)).isTrue();
        assertThat(output.get()).isEqualTo("foo");
    }

    @Test
    public void multi() {
        CommandOutput<String, String, List<String>> output = new ValueListOutput<String, String>(codec);
        ByteBuf buffer = buffer("*2\r\n$-1\r\n$2\r\nok\r\n");
        assertThat(rsm.decode(buffer, output)).isTrue();
        assertThat(output.get()).isEqualTo(Arrays.asList(null, "ok"));
    }

    @Test
    public void multiEmptyArray1() {
        CommandOutput<String, String, List<Object>> output = new NestedMultiOutput<String, String>(codec);
        ByteBuf buffer = buffer("*2\r\n$3\r\nABC\r\n*0\r\n");
        assertThat(rsm.decode(buffer, output)).isTrue();
        assertThat(output.get().get(0)).isEqualTo("ABC");
        assertThat(output.get().get(1)).isEqualTo(Arrays.asList());
        assertThat(output.get().size()).isEqualTo(2);
    }

    @Test
    public void multiEmptyArray2() {
        CommandOutput<String, String, List<Object>> output = new NestedMultiOutput<String, String>(codec);
        ByteBuf buffer = buffer("*2\r\n*0\r\n$3\r\nABC\r\n");
        assertThat(rsm.decode(buffer, output)).isTrue();
        assertThat(output.get().get(0)).isEqualTo(Arrays.asList());
        assertThat(output.get().get(1)).isEqualTo("ABC");
        assertThat(output.get().size()).isEqualTo(2);
    }

    @Test
    public void multiEmptyArray3() {
        CommandOutput<String, String, List<Object>> output = new NestedMultiOutput<String, String>(codec);
        ByteBuf buffer = buffer("*2\r\n*2\r\n$2\r\nAB\r\n$2\r\nXY\r\n*0\r\n");
        assertThat(rsm.decode(buffer, output)).isTrue();
        assertThat(output.get().get(0)).isEqualTo(Arrays.asList("AB", "XY"));
        assertThat(output.get().get(1)).isEqualTo(Arrays.asList());
        assertThat(output.get().size()).isEqualTo(2);
    }

    @Test
    public void partialFirstLine() {
        assertThat(rsm.decode(buffer("+"), output)).isFalse();
        assertThat(rsm.decode(buffer("-"), output)).isFalse();
        assertThat(rsm.decode(buffer(":"), output)).isFalse();
        assertThat(rsm.decode(buffer("$"), output)).isFalse();
        assertThat(rsm.decode(buffer("*"), output)).isFalse();
    }

    @Test
    public void invalidReplyType() {
        assertThatThrownBy(() -> rsm.decode(buffer("="), output)).isInstanceOf(RedisException.class);
    }

    @Test
    public void sillyTestsForEmmaCoverage() {
        assertThat(State.Type.valueOf("SINGLE")).isEqualTo(State.Type.SINGLE);
    }

    protected ByteBuf buffer(String content) {
        return Unpooled.copiedBuffer(content, charset);
    }
}
