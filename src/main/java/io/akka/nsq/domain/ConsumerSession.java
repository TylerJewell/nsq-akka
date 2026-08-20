package io.akka.nsq.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * One consumer's side of the delivery guarantee — SPEC-001 rules 16-23.
 *
 * <p>This is the half nsq puts in its client library rather than in the server, and the
 * port keeps it there: deciding a message is poison, and deciding how fast to ask for
 * more work, both need evidence only the consumer has. The channel is never told that a
 * message failed; it is told to finish it or to put it back.
 */
public record ConsumerSession(
    int maxInFlight,
    int maxAttempts,
    Duration multiplier,
    Duration ceiling,
    Duration defaultRequeueDelay,
    Duration maxRequeueDelay,
    Backoff backoff,
    long gaveUpCount) {

  /** go-nsq's own shipped defaults — question-log #28. */
  public static ConsumerSession withDefaults() {
    return new ConsumerSession(
        1,
        5,
        Duration.ofSeconds(1),
        Duration.ofMinutes(2),
        Duration.ofSeconds(90),
        Duration.ofMinutes(15),
        Backoff.clear(),
        0);
  }

  public enum Outcome {
    /** The handler did its work. */
    SUCCESS,
    /** The handler failed, and the consumer should slow down. */
    FAILURE,
    /** The handler could not take the message now, but this is not a reason to slow down. */
    FAILURE_WITHOUT_BACKOFF
  }

  public enum Action {
    FINISH,
    REQUEUE,
    /** Finish, because the attempt ceiling is exhausted and this message is not coming back. */
    GIVE_UP
  }

  /**
   * @param requeueDelay meaningful only when the action is REQUEUE
   */
  public record Advice(Action action, Duration requeueDelay, int rdy, ConsumerSession session) {}

  /**
   * What to do about one delivery, and what to set the ready count to afterwards.
   *
   * @param attempts the attempt count the channel reported on this delivery
   */
  public Advice decide(Outcome outcome, int attempts, Instant now) {
    if (maxAttempts > 0 && attempts > maxAttempts) {
      var session = new ConsumerSession(maxInFlight, maxAttempts, multiplier, ceiling,
          defaultRequeueDelay, maxRequeueDelay, backoff, gaveUpCount + 1);
      return new Advice(Action.GIVE_UP, Duration.ZERO, session.rdy(now), session);
    }
    var next = switch (outcome) {
      case SUCCESS -> backoff.onOutcome(true, now, multiplier, ceiling);
      case FAILURE -> backoff.onOutcome(false, now, multiplier, ceiling);
      case FAILURE_WITHOUT_BACKOFF -> backoff;
    };
    var session = new ConsumerSession(maxInFlight, maxAttempts, multiplier, ceiling,
        defaultRequeueDelay, maxRequeueDelay, next, gaveUpCount);
    var action = outcome == Outcome.SUCCESS ? Action.FINISH : Action.REQUEUE;
    return new Advice(action, requeueDelay(attempts), session.rdy(now), session);
  }

  public int rdy(Instant now) {
    return backoff.rdy(maxInFlight, now);
  }

  /**
   * Linear in attempts, capped — rule 22. The exponential rule is the ready-count
   * backoff, which is a different mechanism on a different clock.
   */
  Duration requeueDelay(int attempts) {
    var delay = defaultRequeueDelay.multipliedBy(Math.max(1, attempts));
    return delay.compareTo(maxRequeueDelay) > 0 ? maxRequeueDelay : delay;
  }
}
