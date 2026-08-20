package io.akka.nsq.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.akka.nsq.domain.ConsumerSession.Action;
import io.akka.nsq.domain.ConsumerSession.Outcome;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 21, 22, 23 — what the consumer's own side of the guarantee decides. */
class ConsumerSessionTest {

  static final Instant T0 = Instant.parse("2026-08-19T12:00:00Z");

  static ConsumerSession session() {
    return new ConsumerSession(
        5, 3, Duration.ofMillis(200), Duration.ofSeconds(1),
        Duration.ofMillis(500), Duration.ofMillis(1200), Backoff.clear(), 0);
  }

  // --- rule 23: giving up is the consumer's rule --------------------------

  @Test
  void aMessagePastTheAttemptCeilingIsGivenUpOnNotRequeued() {
    var advice = session().decide(Outcome.FAILURE, 4, T0);
    assertEquals(Action.GIVE_UP, advice.action());
    assertEquals(1, advice.session().gaveUpCount());
  }

  @Test
  void theCeilingIsCountedInAttemptsNotFailures() {
    var s = session();
    assertEquals(Action.REQUEUE, s.decide(Outcome.FAILURE, 3, T0).action(), "the third try is allowed");
    assertEquals(Action.GIVE_UP, s.decide(Outcome.FAILURE, 4, T0).action());
  }

  @Test
  void aCeilingOfZeroMeansNeverGiveUp() {
    var s = new ConsumerSession(5, 0, Duration.ofMillis(200), Duration.ofSeconds(1),
        Duration.ofMillis(500), Duration.ofMillis(1200), Backoff.clear(), 0);
    assertEquals(Action.REQUEUE, s.decide(Outcome.FAILURE, 10_000, T0).action());
  }

  @Test
  void givingUpDoesNotRaiseTheBackoffLevel() {
    var advice = session().decide(Outcome.FAILURE, 9, T0);
    assertEquals(0, advice.session().backoff().level(),
        "the message was the problem, not the rate of work");
    assertEquals(5, advice.rdy());
  }

  // --- rule 22: the linear requeue delay ----------------------------------

  @Test
  void theRequeueDelayIsLinearInAttemptsAndCapped() {
    var s = session();
    assertEquals(Duration.ofMillis(500), s.requeueDelay(1));
    assertEquals(Duration.ofMillis(1000), s.requeueDelay(2));
    assertEquals(Duration.ofMillis(1200), s.requeueDelay(3), "capped at max-requeue-delay");
    assertEquals(Duration.ofMillis(1200), s.requeueDelay(50));
  }

  // --- rule 21: a requeue that does not slow the consumer down ------------

  @Test
  void aFailureWithoutBackoffRequeuesAndLeavesTheReadyCountAlone() {
    var advice = session().decide(Outcome.FAILURE_WITHOUT_BACKOFF, 1, T0);
    assertEquals(Action.REQUEUE, advice.action());
    assertEquals(0, advice.session().backoff().level());
    assertEquals(5, advice.rdy(), "still asking for the full count");
  }

  @Test
  void aFailureWithBackoffRequeuesAndStopsAskingForWork() {
    var advice = session().decide(Outcome.FAILURE, 1, T0);
    assertEquals(Action.REQUEUE, advice.action());
    assertEquals(1, advice.session().backoff().level());
    assertEquals(0, advice.rdy());
  }

  @Test
  void aSuccessFinishesTheMessage() {
    var advice = session().decide(Outcome.SUCCESS, 1, T0);
    assertEquals(Action.FINISH, advice.action());
    assertEquals(5, advice.rdy());
  }
}
