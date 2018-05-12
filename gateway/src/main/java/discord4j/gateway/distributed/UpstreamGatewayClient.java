/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J. If not, see <http://www.gnu.org/licenses/>.
 */
package discord4j.gateway.distributed;

import discord4j.common.RateLimiter;
import discord4j.gateway.DefaultGatewayClient;
import discord4j.gateway.GatewayClient;
import discord4j.gateway.GatewayObserver;
import discord4j.gateway.IdentifyOptions;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.dispatch.Dispatch;
import discord4j.gateway.payload.PayloadReader;
import discord4j.gateway.payload.PayloadWriter;
import discord4j.gateway.retry.RetryOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.function.Function;

public class UpstreamGatewayClient implements GatewayClient {

    private final DefaultGatewayClient delegate;
    private final PayloadSink sink;
    private final PayloadSource source;
    private final Logger senderLogger;
    private final Logger receiverLogger;

    public UpstreamGatewayClient(HttpClient httpClient, PayloadReader payloadReader, PayloadWriter payloadWriter,
                                 RetryOptions retryOptions, String token, IdentifyOptions identifyOptions,
                                 GatewayObserver observer, RateLimiter identifyLimiter,
                                 PayloadSink payloadSink, PayloadSource payloadSource) {
        this.delegate = new DefaultGatewayClient(httpClient, payloadReader, payloadWriter, retryOptions, token,
                identifyOptions, observer, identifyLimiter);
        this.sink = payloadSink;
        this.source = payloadSource;
        this.senderLogger = Loggers.getLogger("discord4j.gateway.sender" + identifyOptions.getShardIndex());
        this.receiverLogger = Loggers.getLogger("discord4j.gateway.receiver" + identifyOptions.getShardIndex());
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        return execute(gatewayUrl, GatewayObserver.NOOP_LISTENER);
    }

    @Override
    public Mono<Void> execute(String gatewayUrl, GatewayObserver additionalObserver) {
        Mono<Void> senderFuture = sink.send(receiver())
                .subscribeOn(Schedulers.newSingle("payload-sender"))
                .log(senderLogger)
                .doOnError(t -> senderLogger.error("Sender error", t))
                .then();

        Mono<Void> receiverFuture = source.receive(payloadProcessor())
                .log(receiverLogger)
                .doOnError(t -> senderLogger.error("Receiver error", t))
                .then();

        return Mono.zip(senderFuture, receiverFuture, delegate.execute(gatewayUrl)).then();
    }

    private Function<GatewayPayload<?>, Mono<Void>> payloadProcessor() {
        FluxSink<GatewayPayload<?>> senderSink = sender();
        return payload -> {
            if (senderSink.isCancelled()) {
                return Mono.error(new IllegalStateException("Sender was cancelled"));
            }
            senderSink.next(payload);
            return Mono.empty();
        };
    }

    @Override
    public Mono<Void> close(boolean reconnect) {
        return delegate.close(reconnect);
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return delegate.dispatch();
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return delegate.receiver();
    }

    @Override
    public FluxSink<GatewayPayload<?>> sender() {
        return delegate.sender();
    }

    @Override
    public String getSessionId() {
        return delegate.getSessionId();
    }

    @Override
    public int getSequence() {
        return delegate.getSequence();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    @Override
    public long getResponseTime() {
        return delegate.getResponseTime();
    }
}
