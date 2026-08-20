package io.akka.nsq.domain;

import java.time.Instant;

/**
 * One message on one channel. Each channel of a topic holds its own copy, so the
 * attempt count is per channel and not per publication (SPEC-001 rule 4).
 *
 * @param attempts deliveries so far; 1 on the first delivery, not 0 (rule 1)
 */
public record Message(String id, String body, int attempts, Instant publishedAt) {

  public Message dispatched() {
    return new Message(id, body, attempts + 1, publishedAt);
  }
}
