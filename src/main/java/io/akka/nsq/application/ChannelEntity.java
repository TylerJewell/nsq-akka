package io.akka.nsq.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.nsq.domain.ChannelEvent;
import io.akka.nsq.domain.ChannelState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * One channel's delivery state — SPEC-001 §3, rules 1-15 and 24.
 *
 * <p>The entity id is {@code topic:channel}. All the rules live in {@link ChannelState};
 * this class only decides what to persist and what to answer, so the rules can be tested
 * without a runtime and are tested that way in {@code ChannelStateTest}.
 */
@Component(id = "channel")
public class ChannelEntity extends EventSourcedEntity<ChannelState, ChannelEvent> {

  private final String topic;
  private final String channel;

  public ChannelEntity(EventSourcedEntityContext context) {
    var id = context.entityId();
    int split = id.indexOf(':');
    if (split < 0) {
      throw new IllegalArgumentException("channel id must be topic:channel, got " + id);
    }
    this.topic = id.substring(0, split);
    this.channel = id.substring(split + 1);
  }

  @Override
  public ChannelState emptyState() {
    return ChannelState.of(topic, channel);
  }

  public record Delivery(String messageId, String body, int attempts, Instant deadline) {}

  public record Ready(String clientId, int rdy, Duration msgTimeout) {}

  public record Requeue(String clientId, String messageId, long delayMillis) {}

  /** Names one message and the consumer claiming to hold it. */
  public record Held(String clientId, String messageId) {}

  public record Incoming(String messageId, String body, Instant publishedAt) {}

  public record Stats(
      String topic,
      String channel,
      int depth,
      int inFlight,
      int deferred,
      long messageCount,
      long requeueCount,
      long timeoutCount,
      long finishCount) {}

  /**
   * Accept a message. At-least-once fan-out means this can arrive twice for the same
   * message; the second time is a no-op rather than a second copy.
   */
  public Effect<Done> queue(Incoming message) {
    return persistAll(
        currentState().queue(message.messageId(), message.body(), message.publishedAt()));
  }

  public Effect<Done> setReady(Ready ready) {
    return persistAll(currentState()
        .setReady(ready.clientId(), ready.rdy(), ready.msgTimeout(), Instant.now()));
  }

  public Effect<Done> forget(String clientId) {
    return persistAll(currentState().forget(clientId));
  }

  /** Hand out as many messages as this consumer's ready count still allows. */
  public Effect<List<Delivery>> dispatch(String clientId) {
    var now = Instant.now();
    var events = currentState().dispatch(clientId, now, ChannelState.DEFAULT_MAX_MSG_TIMEOUT);
    if (events.isEmpty()) {
      return effects().reply(List.of());
    }
    var waiting = currentState().ready();
    var deliveries = events.stream()
        .map(ChannelEvent.MessageDispatched.class::cast)
        .map(e -> new Delivery(
            e.messageId(), waiting.get(e.messageId()).body(), e.attempts(), e.deadline()))
        .toList();
    return effects().persistAll(events).thenReply(s -> deliveries);
  }

  public Effect<Done> finish(Held held) {
    return answer(currentState().finish(held.clientId(), held.messageId()));
  }

  public Effect<Done> requeue(Requeue requeue) {
    return answer(currentState().requeue(
        requeue.clientId(),
        requeue.messageId(),
        Duration.ofMillis(requeue.delayMillis()),
        Instant.now(),
        ChannelState.DEFAULT_MAX_REQ_TIMEOUT));
  }

  public Effect<Done> touch(Held held) {
    return answer(currentState().touch(
        held.clientId(), held.messageId(), Instant.now(), ChannelState.DEFAULT_MAX_MSG_TIMEOUT));
  }

  /**
   * Expire everything past its deadline and release everything whose delay has elapsed.
   * Called on the channel's own repeating timer, which is what bounds how late a
   * redelivery can be (rule 24).
   *
   * @return whether this channel still has anything that a later sweep could act on. A
   *     channel holding nothing needs no clock, and saying so is what stops the sweep
   *     from running for the life of the service on a channel nobody uses.
   */
  public Effect<Boolean> sweep() {
    var events = currentState().sweep(Instant.now());
    if (events.isEmpty()) {
      return effects().reply(currentState().hasWork());
    }
    return effects().persistAll(events).thenReply(ChannelState::hasWork);
  }

  public ReadOnlyEffect<Stats> stats() {
    var s = currentState();
    return effects().reply(new Stats(s.topic(), s.channel(), s.depth(), s.inFlight().size(),
        s.deferred().size(), s.messageCount(), s.requeueCount(), s.timeoutCount(), s.finishCount()));
  }

  @Override
  public ChannelState applyEvent(ChannelEvent event) {
    return currentState().apply(event);
  }

  private Effect<Done> answer(ChannelState.Answer answer) {
    return switch (answer) {
      case ChannelState.Answer.Refused refused -> effects().error(refused.reason());
      case ChannelState.Answer.Ok ok -> persistAll(ok.events());
    };
  }

  private Effect<Done> persistAll(List<ChannelEvent> events) {
    if (events.isEmpty()) {
      return effects().reply(Done.getInstance());
    }
    return effects().persistAll(events).thenReply(s -> Done.getInstance());
  }
}
