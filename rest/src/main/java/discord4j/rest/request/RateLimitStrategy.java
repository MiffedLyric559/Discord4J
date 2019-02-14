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

package discord4j.rest.request;

import reactor.netty.http.client.HttpClientResponse;

import java.time.Duration;
import java.util.function.Function;

/**
 * A rate limiting strategy to be applied on {@link RequestStream} buckets.
 */
public interface RateLimitStrategy extends Function<HttpClientResponse, Duration> {

    /**
     * Get a snapshot of the current {@link RateLimitStrategy} used in this bucket.
     *
     * @return a {@link Snapshot} detailing current rate limiting parameters
     */
    Snapshot getSnapshot();

    class Snapshot {

        private final long remaining;
        private final long resetAt;
        private final long date;

        public Snapshot(long remaining, long resetAt, long date) {
            this.remaining = remaining;
            this.resetAt = resetAt;
            this.date = date;
        }

        long getRemaining() {
            return remaining;
        }

        long getResetAt() {
            return resetAt;
        }

        long getDate() {
            return date;
        }
    }
}
