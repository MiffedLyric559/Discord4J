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
import discord4j.gateway.GatewayClient;
import discord4j.gateway.GatewayClientFactory;
import discord4j.gateway.GatewayObserver;
import discord4j.gateway.IdentifyOptions;
import discord4j.gateway.payload.PayloadReader;
import discord4j.gateway.payload.PayloadWriter;
import discord4j.gateway.retry.RetryOptions;
import reactor.netty.http.client.HttpClient;

public class DownstreamGatewayClientFactory implements GatewayClientFactory {

    private final PayloadSink sink;
    private final PayloadSource source;

    public DownstreamGatewayClientFactory(PayloadSink sink, PayloadSource source) {
        this.sink = sink;
        this.source = source;
    }

    @Override
    public GatewayClient getGatewayClient(HttpClient httpClient, PayloadReader payloadReader,
                                          PayloadWriter payloadWriter, RetryOptions retryOptions, String token,
                                          IdentifyOptions identifyOptions, GatewayObserver observer,
                                          RateLimiter rateLimiter) {
        return new DownstreamGatewayClient(sink, source);
    }
}
