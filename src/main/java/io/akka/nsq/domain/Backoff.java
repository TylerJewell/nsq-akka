package io.akka.nsq.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * The consumer's pacing rule — SPEC-001 rules 16-20.
 *
 * <p>A level of zero means no trouble: the consumer asks for its full count. Any level
 * above zero means it asks for nothing until {@code waitingUntil}, and then for exactly
 * one message, to find out whether the trouble has passed.
 *
 * <p>Two things about this rule are easy to get wrong and are both deliberate. The level
 * only rises while the *next* level would still fit under the ceiling, so the wait
 * settles one step below the ceiling rather than at it. And an outcome arriving while the
 * wait is still running moves nothing at all — a burst of failures inside one window
 * counts once, and so does a burst of successes.
 *
 * @param waitingUntil null when the level is zero
 */
public record Backoff(int level, Instant waitingUntil) {

  public static Backoff clear() {
    return new Backoff(0, null);
  }

  public Backoff onOutcome(boolean success, Instant now, Duration multiplier, Duration ceiling) {
    if (waiting(now)) {
      return this;
    }
    int next = success ? Math.max(0, level - 1) : raised(multiplier, ceiling);
    if (next == 0) {
      return clear();
    }
    return new Backoff(next, now.plus(waitFor(next, multiplier, ceiling)));
  }

  /** How many messages to ask for: all of them, none, or exactly one. */
  public int rdy(int maxInFlight, Instant now) {
    if (level == 0) {
      return maxInFlight;
    }
    return waiting(now) ? 0 : 1;
  }

  /** How long this backoff still holds the consumer at nothing, measured from {@code start}. */
  public Duration waitFrom(Instant start) {
    return waitingUntil == null ? Duration.ZERO : Duration.between(start, waitingUntil);
  }

  public boolean waiting(Instant now) {
    return waitingUntil != null && waitingUntil.isAfter(now);
  }

  /**
   * The level rises only while the wait it would produce still fits under the ceiling.
   * The comparison is against the uncapped wait: capping first would make every level
   * fit, and the level would rise forever.
   */
  private int raised(Duration multiplier, Duration ceiling) {
    return uncappedWait(level + 1, multiplier).compareTo(ceiling) <= 0 ? level + 1 : level;
  }

  private static Duration waitFor(int level, Duration multiplier, Duration ceiling) {
    var wait = uncappedWait(level, multiplier);
    return wait.compareTo(ceiling) > 0 ? ceiling : wait;
  }

  private static Duration uncappedWait(int level, Duration multiplier) {
    // 2^level by shifting, bounded so a runaway level cannot overflow into a negative
    // duration the way multiplying by Math.pow would
    return multiplier.multipliedBy(1L << Math.min(level, 40));
  }
}
