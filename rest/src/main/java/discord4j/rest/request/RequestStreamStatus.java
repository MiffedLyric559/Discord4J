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

public class RequestStreamStatus {

    private final boolean globalRateLimited;
    private final int remaining;
    private final long resetAt;
    private final long date;

    public RequestStreamStatus(boolean globalRateLimited, int remaining, long resetAt, long date) {
        this.globalRateLimited = globalRateLimited;
        this.remaining = remaining;
        this.resetAt = resetAt;
        this.date = date;
    }

    public boolean isGlobalRateLimited() {
        return globalRateLimited;
    }

    public boolean isRateLimited() {
        return globalRateLimited || remaining == 0;
    }

    public int getRemaining() {
        return remaining;
    }

    public long getResetAt() {
        return resetAt;
    }

    public long getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "RequestStreamStatus{" +
                "globalRateLimited=" + globalRateLimited +
                ", remaining=" + remaining +
                ", resetAt=" + resetAt +
                ", date=" + date +
                '}';
    }
}
