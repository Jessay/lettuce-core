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

import static io.lettuce.core.protocol.LettuceCharsets.buffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.lettuce.core.*;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StatusOutput;

/**
 * @author Mark Paluch
 */
public class AsyncCommandUnitTests {

    protected RedisCodec<String, String> codec = new Utf8StringCodec();
    protected Command<String, String, String> internal;
    protected AsyncCommand<String, String, String> sut;

    @BeforeEach
    public final void createCommand() {
        CommandOutput<String, String, String> output = new StatusOutput<String, String>(codec);
        internal = new Command<String, String, String>(CommandType.INFO, output, null);
        sut = new AsyncCommand<>(internal);
    }

    @Test
    public void isCancelled() {
        assertThat(sut.isCancelled()).isFalse();
        assertThat(sut.cancel(true)).isTrue();
        assertThat(sut.isCancelled()).isTrue();
        assertThat(sut.cancel(true)).isTrue();
    }

    @Test
    public void isDone() {
        assertThat(sut.isDone()).isFalse();
        sut.complete();
        assertThat(sut.isDone()).isTrue();
    }

    @Test
    public void awaitAllCompleted() {
        sut.complete();
        assertThat(LettuceFutures.awaitAll(5, TimeUnit.MILLISECONDS, sut)).isTrue();
    }

    @Test
    public void awaitAll() {
        assertThat(LettuceFutures.awaitAll(-1, TimeUnit.NANOSECONDS, sut)).isFalse();
    }

    @Test
    public void awaitNotCompleted() {
        assertThatThrownBy(() -> LettuceFutures.awaitOrCancel(sut, 0, TimeUnit.NANOSECONDS)).isInstanceOf(
                RedisCommandTimeoutException.class);
    }

    @Test
    public void awaitWithExecutionException() {
        sut.completeExceptionally(new RedisException("error"));
        assertThatThrownBy(() -> LettuceFutures.awaitOrCancel(sut, 1, TimeUnit.SECONDS)).isInstanceOf(RedisException.class);
    }

    @Test
    public void awaitWithCancelledCommand() {
        sut.cancel();
        assertThatThrownBy(() -> LettuceFutures.awaitOrCancel(sut, 5, TimeUnit.SECONDS)).isInstanceOf(
                CancellationException.class);
    }

    @Test
    public void awaitAllWithExecutionException() {
        sut.completeExceptionally(new RedisCommandExecutionException("error"));

        assertThatThrownBy(() -> LettuceFutures.awaitAll(0, TimeUnit.SECONDS, sut)).isInstanceOf(RedisException.class);
    }

    @Test
    public void getError() {
        sut.getOutput().setError("error");
        assertThat(internal.getError()).isEqualTo("error");
    }

    @Test
    public void getErrorAsync() {
        sut.getOutput().setError("error");
        sut.complete();
        assertThatThrownBy(() -> sut.get()).isInstanceOf(ExecutionException.class);
    }

    @Test
    public void completeExceptionally() {
        sut.completeExceptionally(new RuntimeException("test"));
        assertThat(internal.getError()).isEqualTo("test");

        assertThatThrownBy(() -> sut.get()).isInstanceOf(ExecutionException.class);
    }

    @Test
    public void asyncGet() throws Exception {
        sut.getOutput().set(buffer("one"));
        sut.complete();
        assertThat(sut.get()).isEqualTo("one");
        sut.getOutput().toString();
    }

    @Test
    public void customKeyword() {
        sut = new AsyncCommand<>(
                new Command<String, String, String>(MyKeywords.DUMMY, new StatusOutput<String, String>(codec), null));

        assertThat(sut.toString()).contains(MyKeywords.DUMMY.name());
    }

    @Test
    public void customKeywordWithArgs() {
        sut = new AsyncCommand<>(
                new Command<String, String, String>(MyKeywords.DUMMY, null, new CommandArgs<String, String>(codec)));
        sut.getArgs().add(MyKeywords.DUMMY);
        assertThat(sut.getArgs().toString()).contains(MyKeywords.DUMMY.name());
    }

    @Test
    public void getWithTimeout() throws Exception {
        sut.getOutput().set(buffer("one"));
        sut.complete();

        assertThat(sut.get(0, TimeUnit.MILLISECONDS)).isEqualTo("one");
    }

    @Test
    public void getTimeout() {
        assertThatThrownBy(() -> sut.get(2, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void awaitTimeout() {
        assertThat(sut.await(2, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    public void getInterrupted() {
        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> sut.get()).isInstanceOf(InterruptedException.class);
    }

    @Test
    public void getInterrupted2() {
        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> sut.get(5, TimeUnit.MILLISECONDS)).isInstanceOf(InterruptedException. class);
    }

    @Test
    public void awaitInterrupted2() {
        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> sut.await(5, TimeUnit.MILLISECONDS)).isInstanceOf(RedisCommandInterruptedException. class);
    }

    @Test
    public void outputSubclassOverride1() {
        CommandOutput<String, String, String> output = new CommandOutput<String, String, String>(codec, null) {
            @Override
            public String get() throws RedisException {
                return null;
            }
        };
        assertThatThrownBy(() -> output.set(null)).isInstanceOf(IllegalStateException. class);
    }

    @Test
    public void outputSubclassOverride2() {
        CommandOutput<String, String, String> output = new CommandOutput<String, String, String>(codec, null) {
            @Override
            public String get() throws RedisException {
                return null;
            }
        };
        assertThatThrownBy(() -> output.set(0)).isInstanceOf(IllegalStateException. class);
    }

    @Test
    public void sillyTestsForEmmaCoverage() {
        assertThat(CommandType.valueOf("APPEND")).isEqualTo(CommandType.APPEND);
        assertThat(CommandKeyword.valueOf("AFTER")).isEqualTo(CommandKeyword.AFTER);
    }

    private enum MyKeywords implements ProtocolKeyword {
        DUMMY;

        @Override
        public byte[] getBytes() {
            return name().getBytes();
        }
    }
}
