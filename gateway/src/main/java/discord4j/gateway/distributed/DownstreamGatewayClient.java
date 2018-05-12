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

import discord4j.gateway.GatewayClient;
import discord4j.gateway.GatewayObserver;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.Opcode;
import discord4j.gateway.json.dispatch.Dispatch;
import discord4j.gateway.json.dispatch.Ready;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A worker {@link GatewayClient} node that operates downstream from a leader node, communicating through the given
 * {@link PayloadSource} for receiving and {@link PayloadSink} for sending.
 */
public class DownstreamGatewayClient implements GatewayClient {

    private static final Logger log = Loggers.getLogger("discord4j.gateway.client");

    // basic properties
    private final PayloadSink sink;
    private final PayloadSource source;

    // reactive pipelines
    private final EmitterProcessor<Dispatch> dispatch = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> receiver = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> sender = EmitterProcessor.create(false);
    private final EmitterProcessor<NodeControl> controlReceiver = EmitterProcessor.create(false);
    private final EmitterProcessor<NodeControl> controlSender = EmitterProcessor.create(false);
    private final FluxSink<Dispatch> dispatchSink;
    private final FluxSink<GatewayPayload<?>> receiverSink;
    private final FluxSink<GatewayPayload<?>> senderSink;
    private final FluxSink<NodeControl> controlSenderSink;

    // mutable state
    private final AtomicInteger lastSequence = new AtomicInteger(0);
    private final AtomicReference<String> sessionId = new AtomicReference<>("");

    public DownstreamGatewayClient(PayloadSink sink, PayloadSource source) {
        this.sink = sink;
        this.source = source;
        this.dispatchSink = dispatch.sink(FluxSink.OverflowStrategy.LATEST);
        this.receiverSink = receiver.sink(FluxSink.OverflowStrategy.LATEST);
        this.senderSink = sender.sink(FluxSink.OverflowStrategy.LATEST);
        this.controlSenderSink = controlSender.sink(FluxSink.OverflowStrategy.LATEST);
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        return execute(gatewayUrl, GatewayObserver.NOOP_LISTENER);
    }

    @Override
    public Mono<Void> execute(String gatewayUrl, GatewayObserver additionalObserver) {
        return Mono.defer(() -> {
            Mono<Void> inboundFuture = source
                    .receive(payload -> {
                        if (receiverSink.isCancelled()) {
                            return Mono.error(new IllegalStateException("Sender was cancelled"));
                        }
                        receiverSink.next(payload);
                        return Mono.empty();
                    })
                    .then();

            Mono<Void> receiverFuture = receiver.map(this::updateSequence)
                    .doOnNext(this::handlePayload)
                    .then();

            Mono<Void> controlReceiverFuture = controlReceiver.then();

            Mono<Void> senderFuture = sink.send(sender)
                    .subscribeOn(Schedulers.newSingle("payload-sender"))
                    .then();

            Mono<Void> controlSenderFuture = sink.sendControl(controlSender)
                    .subscribeOn(Schedulers.newSingle("control-sender"))
                    .then();

            return Mono.zip(inboundFuture, receiverFuture, senderFuture, controlReceiverFuture, controlSenderFuture)
                    .doOnError(t -> log.error("Gateway client error: {}", t.toString()))
                    .doOnCancel(() -> close(false))
                    .then();
        });
    }

    private GatewayPayload<?> updateSequence(GatewayPayload<?> payload) {
        if (payload.getSequence() != null) {
            lastSequence.set(payload.getSequence());
        }
        return payload;
    }

    private void handlePayload(GatewayPayload<?> payload) {
        if (Opcode.DISPATCH.equals(payload.getOp())) {
            if (payload.getData() instanceof Ready) {
                String newSessionId = ((Ready) payload.getData()).getSessionId();
                sessionId.set(newSessionId);
            }
            if (payload.getData() != null) {
                dispatchSink.next((Dispatch) payload.getData());
            }
        }
    }

    @Override
    public Mono<Void> close(boolean reconnect) {
        if (reconnect) {
            NodeControl reconnectOp = new NodeControl();
            reconnectOp.setOp(NodeControl.Op.RECONNECT);
            controlSenderSink.next(reconnectOp);
            return controlReceiver.filter(nodeControl -> NodeControl.Op.RECONNECT.equals(nodeControl.getOp()))
                    .next()
                    .then();
        } else {
            NodeControl closeOp = new NodeControl();
            closeOp.setOp(NodeControl.Op.CLOSE);
            controlSenderSink.next(closeOp);
            return controlReceiver.filter(nodeControl -> NodeControl.Op.CLOSE.equals(nodeControl.getOp()))
                    .next()
                    .then();
        }
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return dispatch;
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return receiver;
    }

    @Override
    public FluxSink<GatewayPayload<?>> sender() {
        return senderSink;
    }

    @Override
    public String getSessionId() {
        return sessionId.get();
    }

    @Override
    public int getSequence() {
        return lastSequence.get();
    }

    @Override
    public boolean isConnected() {
        // TODO: implement using control frames
        return true;
    }

    @Override
    public long getResponseTime() {
        // TODO: implement using control frames
        return 0;
    }
}
