package io.akka.nsq.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.nsq.domain.ConsumerSession;
import java.time.Duration;
import java.time.Instant;

/**
 * One consumer's pacing state — SPEC-001 rules 16-23.
 *
 * <p>Kept apart from {@link ChannelEntity} on purpose: in nsq the backoff level and the
 * attempt ceiling live in the client library and the server never learns that a handler
 * failed. Splitting them here keeps that boundary visible rather than implied (§4
 * decision 5). The entity id is {@code topic:channel:clientId}.
 */
@Component(id = "consumer-session")
public class ConsumerSessionEntity extends KeyValueEntity<ConsumerSession> {

  @Override
  public ConsumerSession emptyState() {
    return ConsumerSession.withDefaults();
  }

  public record Settings(
      int maxInFlight,
      int maxAttempts,
      long backoffMultiplierMillis,
      long maxBackoffMillis,
      long defaultRequeueDelayMillis,
      long maxRequeueDelayMillis) {}

  public record Report(ConsumerSession.Outcome outcome, int attempts) {}

  public record Decision(ConsumerSession.Action action, long requeueDelayMillis, int rdy) {}

  public Effect<Integer> configure(Settings settings) {
    var session = new ConsumerSession(
        settings.maxInFlight(),
        settings.maxAttempts(),
        Duration.ofMillis(settings.backoffMultiplierMillis()),
        Duration.ofMillis(settings.maxBackoffMillis()),
        Duration.ofMillis(settings.defaultRequeueDelayMillis()),
        Duration.ofMillis(settings.maxRequeueDelayMillis()),
        io.akka.nsq.domain.Backoff.clear(),
        currentState().gaveUpCount());
    return effects().updateState(session).thenReply(session.rdy(Instant.now()));
  }

  /** What to do about one delivery, and what the ready count becomes. */
  public Effect<Decision> report(Report report) {
    var advice = currentState().decide(report.outcome(), report.attempts(), Instant.now());
    return effects()
        .updateState(advice.session())
        .thenReply(new Decision(
            advice.action(), advice.requeueDelay().toMillis(), advice.rdy()));
  }

  /**
   * The ready count this consumer should be at now. A backoff wait expires on the clock
   * rather than on an event, so the answer changes without anything being reported.
   */
  public ReadOnlyEffect<Integer> readyCount() {
    return effects().reply(currentState().rdy(Instant.now()));
  }

  public ReadOnlyEffect<ConsumerSession> get() {
    return effects().reply(currentState());
  }
}
