package io.akka.nsq.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * What the channel knows about one consumer: how many unanswered messages it will hold,
 * how many it holds now, the deadline it asked for, and when it was last heard from.
 *
 * <p>Why it wants that ready count is not the channel's business — that lives in
 * {@link Backoff}, on the consumer's side of the line (SPEC-001 §2).
 */
public record ClientState(int rdy, int inFlightCount, Duration msgTimeout, Instant lastSeenAt) {

  public ClientState withRdy(int newRdy, Duration newMsgTimeout, Instant now) {
    return new ClientState(newRdy, inFlightCount, newMsgTimeout, now);
  }

  public ClientState took() {
    return new ClientState(rdy, inFlightCount + 1, msgTimeout, lastSeenAt);
  }

  public ClientState released() {
    return new ClientState(rdy, Math.max(0, inFlightCount - 1), msgTimeout, lastSeenAt);
  }
}
