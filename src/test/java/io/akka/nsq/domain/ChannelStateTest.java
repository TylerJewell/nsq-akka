package io.akka.nsq.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.nsq.domain.ChannelState.Answer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The delivery rules of SPEC-001 §3, against the state machine that holds them.
 *
 * <p>Every rule number below is the spec's, and every one traces to a row of
 * docs/question-log.md that was checked by running nsq itself.
 */
class ChannelStateTest {

  static final Duration MSG_TIMEOUT = Duration.ofSeconds(60);
  static final Duration MAX_MSG_TIMEOUT = Duration.ofMinutes(15);
  static final Duration MAX_REQ_TIMEOUT = Duration.ofHours(1);
  static final Instant T0 = Instant.parse("2026-08-19T12:00:00Z");

  static ChannelState empty() {
    return ChannelState.of("orders", "workers");
  }

  static ChannelState apply(ChannelState state, List<ChannelEvent> events) {
    var s = state;
    for (var e : events) {
      s = s.apply(e);
    }
    return s;
  }

  static ChannelState queued(ChannelState state, String... bodies) {
    var s = state;
    for (var body : bodies) {
      s = apply(s, s.queue(body, body, T0));
    }
    return s;
  }

  static ChannelState ready(ChannelState state, String clientId, int rdy) {
    return apply(state, state.setReady(clientId, rdy, MSG_TIMEOUT, T0));
  }

  static List<ChannelEvent> dispatch(ChannelState state, String clientId, Instant now) {
    return state.dispatch(clientId, now, MAX_MSG_TIMEOUT);
  }

  // --- rule 1: the attempt count ------------------------------------------

  @Test
  void firstDispatchReportsOneAttempt() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    var events = dispatch(s, "c1", T0);
    var dispatched = (ChannelEvent.MessageDispatched) events.get(0);
    assertEquals(1, dispatched.attempts());
  }

  @Test
  void everyRedispatchCountsAnotherAttempt() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s, ((Answer.Ok) s.requeue("c1", "a", Duration.ZERO, T0, MAX_REQ_TIMEOUT)).events());
    var second = (ChannelEvent.MessageDispatched) dispatch(s, "c1", T0).get(0);
    assertEquals(2, second.attempts());

    s = apply(s, List.of(second));
    s = apply(s, s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1)));
    var third = (ChannelEvent.MessageDispatched) dispatch(s, "c1", T0).get(0);
    assertEquals(3, third.attempts(), "a timeout redelivery counts an attempt too");
  }

  @Test
  void aRedispatchToAnotherConsumerStillCountsAnAttempt() {
    var s = ready(ready(queued(empty(), "a"), "c1", 1), "c2", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s, s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1)));
    var again = (ChannelEvent.MessageDispatched) dispatch(s, "c2", T0).get(0);
    assertEquals(2, again.attempts());
    assertEquals("c2", again.clientId());
  }

  // --- rules 2, 3: RDY ----------------------------------------------------

  @Test
  void aConsumerIsNeverHandedMoreThanItsReadyCount() {
    var s = ready(queued(empty(), "a", "b", "c"), "c1", 2);
    assertEquals(2, dispatch(s, "c1", T0).size());
  }

  @Test
  void readyZeroHandsOutNothing() {
    var s = ready(queued(empty(), "a", "b"), "c1", 0);
    assertTrue(dispatch(s, "c1", T0).isEmpty());
  }

  @Test
  void whatIsAlreadyHeldCountsAgainstTheReadyCount() {
    var s = ready(queued(empty(), "a", "b", "c"), "c1", 2);
    s = apply(s, dispatch(s, "c1", T0));
    assertTrue(dispatch(s, "c1", T0).isEmpty(), "both slots are occupied");
  }

  @Test
  void loweringReadyDoesNotRecallWhatIsAlreadyInFlight() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = ready(s, "c1", 0);
    assertEquals(1, s.inFlight().size(), "the dispatched message is still owed an answer");
    assertInstanceOf(Answer.Ok.class, s.finish("c1", "a"));
  }

  // --- rule 4: channels are independent -----------------------------------

  @Test
  void eachChannelKeepsItsOwnAttemptCount() {
    var one = ready(queued(ChannelState.of("orders", "a"), "m"), "c1", 1);
    var two = ready(queued(ChannelState.of("orders", "b"), "m"), "c1", 1);
    one = apply(one, dispatch(one, "c1", T0));
    one = apply(one, ((Answer.Ok) one.requeue("c1", "m", Duration.ZERO, T0, MAX_REQ_TIMEOUT)).events());
    two = apply(two, dispatch(two, "c1", T0));

    var onAgain = (ChannelEvent.MessageDispatched) dispatch(one, "c1", T0).get(0);
    assertEquals(2, onAgain.attempts());
    assertEquals(1, two.inFlight().get("m").message().attempts(),
        "answering on one channel leaves the other untouched");
  }

  // --- rules 5, 6, 7, 8: the three answers --------------------------------

  @Test
  void finishRemovesTheMessage() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s, ((Answer.Ok) s.finish("c1", "a")).events());
    assertEquals(0, s.depth());
    assertTrue(s.inFlight().isEmpty());
    assertEquals(1, s.finishCount());
  }

  @Test
  void aRequeuedMessageGoesBehindWhatIsAlreadyWaiting() {
    var s = ready(queued(empty(), "o0", "o1", "o2"), "c1", 1);
    var order = new java.util.ArrayList<String>();
    s = apply(s, dispatch(s, "c1", T0));
    order.add("o0");
    s = apply(s, ((Answer.Ok) s.requeue("c1", "o0", Duration.ZERO, T0, MAX_REQ_TIMEOUT)).events());
    for (int i = 0; i < 3; i++) {
      var events = dispatch(s, "c1", T0);
      var d = (ChannelEvent.MessageDispatched) events.get(0);
      order.add(d.messageId());
      s = apply(s, events);
      s = apply(s, ((Answer.Ok) s.finish("c1", d.messageId())).events());
    }
    assertEquals(List.of("o0", "o1", "o2", "o0"), order);
  }

  @Test
  void aDelayedRequeueIsHeldOutOfTheQueueUntilItsTime() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s,
        ((Answer.Ok) s.requeue("c1", "a", Duration.ofSeconds(30), T0, MAX_REQ_TIMEOUT)).events());

    assertEquals(1, s.deferred().size());
    assertTrue(dispatch(s, "c1", T0.plusSeconds(29)).isEmpty(), "still deferred");

    s = apply(s, s.sweep(T0.plusSeconds(31)));
    assertTrue(s.deferred().isEmpty());
    assertEquals(1, dispatch(s, "c1", T0.plusSeconds(31)).size());
  }

  @Test
  void aRequeueDelayPastTheMaximumIsClampedAndAccepted() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    var answer = s.requeue("c1", "a", Duration.ofDays(1), T0, MAX_REQ_TIMEOUT);
    assertInstanceOf(Answer.Ok.class, answer, "clamped, not refused");
    s = apply(s, ((Answer.Ok) answer).events());
    assertEquals(T0.plus(MAX_REQ_TIMEOUT), s.deferred().get("a").readyAt());
  }

  // --- rules 9, 10: touch and its ceiling ---------------------------------

  @Test
  void touchRestartsTheDeadlineFromNow() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    var later = T0.plusSeconds(50);
    s = apply(s, ((Answer.Ok) s.touch("c1", "a", later, MAX_MSG_TIMEOUT)).events());
    assertEquals(later.plus(MSG_TIMEOUT), s.inFlight().get("a").deadline());
    assertTrue(s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1)).isEmpty(), "no longer expired");
  }

  @Test
  void touchCannotHoldAMessagePastTheCeilingFromTheDeliveryBeingExtended() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    var nearCeiling = T0.plus(MAX_MSG_TIMEOUT).minusSeconds(10);
    s = apply(s, ((Answer.Ok) s.touch("c1", "a", nearCeiling, MAX_MSG_TIMEOUT)).events());
    assertEquals(T0.plus(MAX_MSG_TIMEOUT), s.inFlight().get("a").deadline(),
        "the extension stops at the ceiling, measured from this delivery");

    var past = T0.plus(MAX_MSG_TIMEOUT).plusSeconds(1);
    s = apply(s, ((Answer.Ok) s.touch("c1", "a", past, MAX_MSG_TIMEOUT)).events());
    assertEquals(1, s.sweep(past).size(), "touched, and it times out anyway");
  }

  // --- rule 11: answers for messages the caller does not hold -------------

  @Test
  void anAnswerForAnUnknownMessageIsRefused() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    assertInstanceOf(Answer.Refused.class, s.finish("c1", "nope"));
    assertInstanceOf(Answer.Refused.class, s.requeue("c1", "nope", Duration.ZERO, T0, MAX_REQ_TIMEOUT));
    assertInstanceOf(Answer.Refused.class, s.touch("c1", "nope", T0, MAX_MSG_TIMEOUT));
  }

  @Test
  void anAnswerForAnotherConsumersMessageIsRefusedAndChangesNothing() {
    var s = ready(ready(queued(empty(), "a"), "c1", 1), "c2", 1);
    s = apply(s, dispatch(s, "c1", T0));
    assertInstanceOf(Answer.Refused.class, s.finish("c2", "a"));
    assertEquals(1, s.inFlight().size());
    assertInstanceOf(Answer.Ok.class, s.finish("c1", "a"), "the owner can still answer");
  }

  // --- rules 12, 13, 15: the answer given when there is none ---------------

  @Test
  void anUnansweredMessageIsRedeliveredNotDropped() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    var expiry = s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1));
    assertEquals(1, expiry.size());
    assertInstanceOf(ChannelEvent.MessageTimedOut.class, expiry.get(0));
    s = apply(s, expiry);
    assertEquals(1, s.depth());
    assertEquals(1, s.timeoutCount());
  }

  @Test
  void aMessageIsNotExpiredBeforeItsDeadline() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    assertTrue(s.sweep(T0.plus(MSG_TIMEOUT).minusSeconds(1)).isEmpty());
  }

  @Test
  void aConsumerThatDisappearsDoesNotReleaseItsMessagesEarly() {
    var s = ready(ready(queued(empty(), "a"), "c1", 1), "c2", 1);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s, s.forget("c1"));
    assertEquals(1, s.inFlight().size(), "still in flight, still owed an answer");
    assertTrue(dispatch(s, "c2", T0).isEmpty(), "the other consumer cannot have it yet");

    s = apply(s, s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1)));
    assertEquals(1, dispatch(s, "c2", T0.plus(MSG_TIMEOUT).plusSeconds(1)).size());
  }

  @Test
  void theDeadlineIsTheConsumersOwn() {
    var s = queued(empty(), "a");
    s = apply(s, s.setReady("slow", 1, Duration.ofSeconds(600), T0));
    s = apply(s, s.setReady("quick", 1, Duration.ofSeconds(5), T0));
    var d = (ChannelEvent.MessageDispatched) dispatch(s, "quick", T0).get(0);
    assertEquals(T0.plusSeconds(5), d.deadline());
  }

  @Test
  void aConsumerDeadlineIsBoundedByTheChannelCeiling() {
    var s = queued(empty(), "a");
    s = apply(s, s.setReady("greedy", 1, Duration.ofDays(1), T0));
    var d = (ChannelEvent.MessageDispatched) dispatch(s, "greedy", T0).get(0);
    assertEquals(T0.plus(MAX_MSG_TIMEOUT), d.deadline());
  }

  // --- rule 14: the channel never gives up --------------------------------

  @Test
  void theChannelNeverGivesUpOnAMessage() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    for (int i = 0; i < 1000; i++) {
      var events = dispatch(s, "c1", T0);
      assertEquals(1, events.size(), "still dispatched after " + i + " requeues");
      s = apply(s, events);
      s = apply(s, ((Answer.Ok) s.requeue("c1", "a", Duration.ZERO, T0, MAX_REQ_TIMEOUT)).events());
    }
    var last = (ChannelEvent.MessageDispatched) dispatch(s, "c1", T0).get(0);
    assertEquals(1001, last.attempts());
  }

  // --- queueing is idempotent, because fan-out is at-least-once ------------

  @Test
  void queueingTheSameMessageTwiceQueuesItOnce() {
    var s = queued(empty(), "a");
    assertTrue(s.queue("a", "a", T0).isEmpty(), "already known");
    s = ready(s, "c1", 5);
    s = apply(s, dispatch(s, "c1", T0));
    assertTrue(s.queue("a", "a", T0).isEmpty(), "known while in flight too");
    assertEquals(1, s.messageCount());
  }

  @Test
  void twoMessagesExpiringInOneSweepAreQueuedInTheOrderTheyWereDelivered() {
    var s = ready(queued(empty(), "first", "second", "third"), "c1", 2);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s, s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1)));
    assertEquals(List.of("third", "first", "second"), List.copyOf(s.ready().keySet()),
        "both go behind what was still waiting, and keep their order relative to each other");
  }

  @Test
  void aConsumerThatSaysNothingForLongEnoughIsForgotten() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    var later = T0.plus(ChannelState.CLIENT_IDLE_LIMIT).plusSeconds(1);
    assertTrue(s.sweep(T0.plusSeconds(30)).isEmpty(), "not yet");
    s = apply(s, s.sweep(later));
    assertTrue(s.clients().isEmpty());
    assertEquals(1, s.depth(), "its ready count is forgotten; the work is not");
  }

  @Test
  void aConsumerHoldingAMessageIsNotForgottenWhileItStillOwesAnAnswer() {
    var s = ready(queued(empty(), "a"), "c1", 1);
    s = apply(s, dispatch(s, "c1", T0));
    var later = T0.plus(ChannelState.CLIENT_IDLE_LIMIT).plusSeconds(1);
    var events = s.sweep(later);
    assertEquals(1, events.size());
    assertInstanceOf(ChannelEvent.MessageTimedOut.class, events.get(0),
        "the message comes back first; the consumer is forgotten on a later sweep");
  }

  @Test
  void sweepReleasesDeferredAndExpiresInFlightInOnePass() {
    var s = ready(queued(empty(), "a", "b"), "c1", 2);
    s = apply(s, dispatch(s, "c1", T0));
    s = apply(s,
        ((Answer.Ok) s.requeue("c1", "b", Duration.ofSeconds(10), T0, MAX_REQ_TIMEOUT)).events());

    var events = s.sweep(T0.plus(MSG_TIMEOUT).plusSeconds(1));
    assertEquals(2, events.size());
    s = apply(s, events);
    assertEquals(2, s.depth());
    assertTrue(s.deferred().isEmpty());
    assertFalse(s.inFlight().containsKey("a"));
  }
}
