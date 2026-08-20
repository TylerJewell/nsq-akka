package io.akka.nsq.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The pacing rules of SPEC-001 §3 (rules 16-20), against the level machine that holds
 * them.
 *
 * <p>The numbers below are the ones the real go-nsq consumer produced in
 * docs/question-log.md rows 19-24: a 200ms multiplier and a 1s ceiling, which makes
 * level 1 a 400ms wait, level 2 an 800ms wait, and level 3 unreachable.
 */
class BackoffTest {

  static final Duration MULTIPLIER = Duration.ofMillis(200);
  static final Duration CEILING = Duration.ofSeconds(1);
  static final int MAX_IN_FLIGHT = 5;
  static final Instant T0 = Instant.parse("2026-08-19T12:00:00Z");

  static Backoff none() {
    return Backoff.clear();
  }

  static Backoff fail(Backoff b, Instant now) {
    return b.onOutcome(false, now, MULTIPLIER, CEILING);
  }

  static Backoff succeed(Backoff b, Instant now) {
    return b.onOutcome(true, now, MULTIPLIER, CEILING);
  }

  // --- rule 16 ------------------------------------------------------------

  @Test
  void aClearConsumerRunsAtItsFullReadyCount() {
    assertEquals(MAX_IN_FLIGHT, none().rdy(MAX_IN_FLIGHT, T0));
  }

  @Test
  void aFailureDropsReadyToZeroForTheBackoffWait() {
    var b = fail(none(), T0);
    assertEquals(1, b.level());
    assertEquals(0, b.rdy(MAX_IN_FLIGHT, T0));
    assertEquals(0, b.rdy(MAX_IN_FLIGHT, T0.plusMillis(399)));
  }

  @Test
  void afterTheWaitTheConsumerAsksForOneMessageNotItsFullCount() {
    var b = fail(none(), T0);
    assertEquals(1, b.rdy(MAX_IN_FLIGHT, T0.plusMillis(400)),
        "one message, to test whether the trouble has passed");
  }

  // --- rule 20: the wait doubles per level --------------------------------

  @Test
  void eachCountedFailureDoublesTheWait() {
    var b = fail(none(), T0);
    assertEquals(Duration.ofMillis(400), b.waitFrom(T0));

    var t1 = T0.plusMillis(400);
    b = fail(b, t1);
    assertEquals(2, b.level());
    assertEquals(Duration.ofMillis(800), b.waitFrom(t1));
  }

  // --- rule 18: the ceiling stops the level, not the wait -----------------

  @Test
  void theLevelStopsRisingOnceTheNextOneWouldPassTheCeiling() {
    var b = fail(none(), T0);
    var t = T0.plusMillis(400);
    b = fail(b, t);
    for (int i = 0; i < 5; i++) {
      t = t.plusMillis(800);
      b = fail(b, t);
      assertEquals(2, b.level(), "level 3 would be 1600ms, past the 1000ms ceiling");
      assertEquals(Duration.ofMillis(800), b.waitFrom(t),
          "the wait settles below the ceiling, not at it");
    }
  }

  // --- rule 19: outcomes inside the wait are not counted ------------------

  @Test
  void aFailureInsideTheWaitDoesNotRaiseTheLevel() {
    var b = fail(none(), T0);
    b = fail(b, T0.plusMillis(100));
    b = fail(b, T0.plusMillis(200));
    assertEquals(1, b.level(), "three failures in one window raise the level once");
  }

  @Test
  void aSuccessInsideTheWaitDoesNotLowerTheLevel() {
    var b = fail(fail(none(), T0), T0.plusMillis(400));
    assertEquals(2, b.level());
    var inside = succeed(b, T0.plusMillis(500));
    assertEquals(2, inside.level());
  }

  // --- rule 17: one success is not recovery -------------------------------

  @Test
  void oneSuccessLowersTheLevelByOneAndNoMore() {
    var t = T0;
    var b = fail(none(), t);
    t = t.plusMillis(400);
    b = fail(b, t);
    assertEquals(2, b.level());

    t = t.plusMillis(800);
    b = succeed(b, t);
    assertEquals(1, b.level());
    assertEquals(0, b.rdy(MAX_IN_FLIGHT, t), "still backing off, still not at full ready");

    t = t.plusMillis(400);
    b = succeed(b, t);
    assertEquals(0, b.level());
    assertEquals(MAX_IN_FLIGHT, b.rdy(MAX_IN_FLIGHT, t), "recovery cost as many successes as failures");
  }

  @Test
  void aSuccessWithNoBackoffRunningChangesNothing() {
    assertEquals(0, succeed(none(), T0).level());
    assertEquals(MAX_IN_FLIGHT, succeed(none(), T0).rdy(MAX_IN_FLIGHT, T0));
  }
}
