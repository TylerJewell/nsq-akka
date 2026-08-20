package io.akka.nsq.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One channel's whole delivery state, and the rules that move it — SPEC-001 §3.
 *
 * <p>Every method here is pure: a decision method returns the events its decision
 * produces without changing anything, and {@link #apply} is the only thing that builds a
 * new state. That split is what lets the rules be tested at the same rung the source was
 * checked at, without a runtime in the way.
 *
 * <p>A message is in exactly one of three places: waiting in {@code ready}, handed out in
 * {@code inFlight}, or held back in {@code deferred}. Nothing is ever in two, and nothing
 * leaves all three except by being finished.
 *
 * <p>All four maps are insertion-ordered and wrapped rather than rehashed. Order is part
 * of the contract, not a convenience: {@code ready} is a queue, and the order two messages
 * expiring in the same sweep are put back in is the order they were delivered in.
 *
 * @param ready messages waiting, oldest first, keyed by message id
 * @param clients every consumer heard from recently; see {@link #evictable}
 */
public record ChannelState(
    String topic,
    String channel,
    Map<String, Message> ready,
    Map<String, InFlight> inFlight,
    Map<String, DeferredMessage> deferred,
    Map<String, ClientState> clients,
    long messageCount,
    long requeueCount,
    long timeoutCount,
    long finishCount) {

  public static final Duration DEFAULT_MSG_TIMEOUT = Duration.ofSeconds(60);
  public static final Duration DEFAULT_MAX_MSG_TIMEOUT = Duration.ofMinutes(15);
  public static final Duration DEFAULT_MAX_REQ_TIMEOUT = Duration.ofHours(1);

  /**
   * How long a consumer that says nothing at all is remembered for. nsq drops a consumer
   * when its connection drops; this port has no connection to lose, so silence stands in
   * for it. Long enough that no ordinary backoff wait reaches it.
   */
  public static final Duration CLIENT_IDLE_LIMIT = Duration.ofMinutes(10);

  public static ChannelState of(String topic, String channel) {
    return new ChannelState(topic, channel, Map.of(), Map.of(), Map.of(), Map.of(), 0, 0, 0, 0);
  }

  /** Messages waiting to be handed out. In flight and deferred are counted separately. */
  public int depth() {
    return ready.size();
  }

  /**
   * Whether a later sweep could act on anything here. A channel with no messages anywhere
   * and no consumer to forget has nothing for a clock to do.
   */
  public boolean hasWork() {
    return !ready.isEmpty() || !inFlight.isEmpty() || !deferred.isEmpty() || !clients.isEmpty();
  }

  /** The answer to a consumer's finish, requeue or touch. */
  public sealed interface Answer {
    record Ok(List<ChannelEvent> events) implements Answer {}

    record Refused(String reason) implements Answer {}
  }

  // --- decisions ----------------------------------------------------------

  /**
   * Accept a message onto this channel. Fan-out is at-least-once, so a message already
   * known here — waiting, in flight or deferred — produces nothing rather than a second
   * copy.
   */
  public List<ChannelEvent> queue(String messageId, String body, Instant now) {
    if (knows(messageId)) {
      return List.of();
    }
    return List.of(new ChannelEvent.MessageQueued(messageId, body, now));
  }

  public List<ChannelEvent> setReady(
      String clientId, int rdy, Duration msgTimeout, Instant now) {
    return List.of(new ChannelEvent.ClientReadySet(clientId, Math.max(0, rdy), msgTimeout, now));
  }

  public List<ChannelEvent> forget(String clientId) {
    return clients.containsKey(clientId)
        ? List.of(new ChannelEvent.ClientForgotten(clientId))
        : List.of();
  }

  /**
   * Hand out as many waiting messages as the consumer's ready count still allows
   * (rules 2, 3). A consumer that has never been heard from has no ready count and so
   * gets nothing.
   */
  public List<ChannelEvent> dispatch(String clientId, Instant now, Duration maxMsgTimeout) {
    var client = clients.get(clientId);
    if (client == null) {
      return List.of();
    }
    int slots = Math.min(client.rdy() - client.inFlightCount(), ready.size());
    if (slots <= 0) {
      return List.of();
    }
    var deadline = now.plus(cappedTimeout(client.msgTimeout(), maxMsgTimeout));
    var events = new ArrayList<ChannelEvent>(slots);
    for (var message : ready.values()) {
      if (events.size() == slots) {
        break;
      }
      events.add(new ChannelEvent.MessageDispatched(
          message.id(), clientId, message.attempts() + 1, deadline, now));
    }
    return events;
  }

  public Answer finish(String clientId, String messageId) {
    var held = heldBy(clientId, messageId);
    if (held instanceof Answer.Refused refused) {
      return refused;
    }
    return new Answer.Ok(List.of(new ChannelEvent.MessageFinished(messageId)));
  }

  /**
   * Put a message back, either at the end of the queue or after a delay (rules 6, 7). A
   * delay past the maximum is clamped and accepted, never refused (rule 8).
   */
  public Answer requeue(
      String clientId, String messageId, Duration delay, Instant now, Duration maxReqTimeout) {
    var held = heldBy(clientId, messageId);
    if (held instanceof Answer.Refused refused) {
      return refused;
    }
    var clamped = clamp(delay, maxReqTimeout);
    if (clamped.isZero()) {
      return new Answer.Ok(List.of(new ChannelEvent.MessageRequeued(messageId)));
    }
    return new Answer.Ok(List.of(new ChannelEvent.MessageDeferred(messageId, now.plus(clamped))));
  }

  /**
   * Restart the deadline from now (rule 9), except that the extension may not carry the
   * message past {@code maxMsgTimeout} measured from the delivery being extended
   * (rule 10).
   */
  public Answer touch(String clientId, String messageId, Instant now, Duration maxMsgTimeout) {
    var held = heldBy(clientId, messageId);
    if (held instanceof Answer.Refused refused) {
      return refused;
    }
    var current = inFlight.get(messageId);
    var client = clients.get(clientId);
    var wanted = now.plus(cappedTimeout(client.msgTimeout(), maxMsgTimeout));
    var ceiling = current.deliveredAt().plus(maxMsgTimeout);
    var deadline = wanted.isAfter(ceiling) ? ceiling : wanted;
    return new Answer.Ok(List.of(new ChannelEvent.DeadlineExtended(messageId, deadline)));
  }

  /**
   * The whole of the timeout mechanism: everything past its deadline goes back to the
   * queue, everything whose delay has elapsed comes out of hiding, and consumers nothing
   * has been heard from are dropped. Nothing is dropped that a consumer still owes an
   * answer for (rules 12, 24).
   */
  public List<ChannelEvent> sweep(Instant now) {
    var events = new ArrayList<ChannelEvent>();
    for (var entry : inFlight.entrySet()) {
      if (!entry.getValue().deadline().isAfter(now)) {
        events.add(new ChannelEvent.MessageTimedOut(entry.getKey()));
      }
    }
    for (var entry : deferred.entrySet()) {
      if (!entry.getValue().readyAt().isAfter(now)) {
        events.add(new ChannelEvent.DeferredReleased(entry.getKey()));
      }
    }
    for (var clientId : evictable(now)) {
      events.add(new ChannelEvent.ClientForgotten(clientId));
    }
    return events;
  }

  /**
   * Consumers that have said nothing for {@link #CLIENT_IDLE_LIMIT} and hold nothing.
   * Without this the client table is the one part of the state that only ever grows: a
   * consumer that never calls back leaves an entry behind for the life of the channel.
   */
  private List<String> evictable(Instant now) {
    var stale = new ArrayList<String>();
    for (var entry : clients.entrySet()) {
      var client = entry.getValue();
      if (client.inFlightCount() == 0
          && client.lastSeenAt().plus(CLIENT_IDLE_LIMIT).isBefore(now)) {
        stale.add(entry.getKey());
      }
    }
    return stale;
  }

  // --- applying -----------------------------------------------------------

  /**
   * Never throws and never validates: it runs on every replay, and an entity that cannot
   * finish replaying cannot be recovered by any later command. An event naming a message
   * this state does not hold describes no change to it, so it is a no-op.
   */
  public ChannelState apply(ChannelEvent event) {
    return switch (event) {
      case ChannelEvent.MessageQueued e ->
          withReady(added(ready, e.messageId(), new Message(e.messageId(), e.body(), 0, e.publishedAt())))
              .withCounts(messageCount + 1, requeueCount, timeoutCount, finishCount);
      case ChannelEvent.MessageDispatched e -> {
        var message = ready.get(e.messageId());
        if (message == null) {
          yield this;
        }
        yield new ChannelState(topic, channel,
            removed(ready, e.messageId()),
            added(inFlight, e.messageId(),
                new InFlight(message.dispatched(), e.clientId(), e.deadline(), e.deliveredAt())),
            deferred, adjust(e.clientId(), ClientState::took),
            messageCount, requeueCount, timeoutCount, finishCount);
      }
      case ChannelEvent.MessageFinished e -> {
        var flying = inFlight.get(e.messageId());
        if (flying == null) {
          yield this;
        }
        yield new ChannelState(topic, channel, ready, removed(inFlight, e.messageId()), deferred,
            adjust(flying.clientId(), ClientState::released),
            messageCount, requeueCount, timeoutCount, finishCount + 1);
      }
      case ChannelEvent.MessageRequeued e -> backToQueue(e.messageId(), requeueCount + 1, timeoutCount);
      case ChannelEvent.MessageTimedOut e -> backToQueue(e.messageId(), requeueCount, timeoutCount + 1);
      case ChannelEvent.MessageDeferred e -> {
        var flying = inFlight.get(e.messageId());
        if (flying == null) {
          yield this;
        }
        yield new ChannelState(topic, channel, ready, removed(inFlight, e.messageId()),
            added(deferred, e.messageId(), new DeferredMessage(flying.message(), e.readyAt())),
            adjust(flying.clientId(), ClientState::released),
            messageCount, requeueCount + 1, timeoutCount, finishCount);
      }
      case ChannelEvent.DeferredReleased e -> {
        var held = deferred.get(e.messageId());
        if (held == null) {
          yield this;
        }
        yield new ChannelState(topic, channel, added(ready, e.messageId(), held.message()),
            inFlight, removed(deferred, e.messageId()), clients,
            messageCount, requeueCount, timeoutCount, finishCount);
      }
      case ChannelEvent.DeadlineExtended e -> {
        var flying = inFlight.get(e.messageId());
        if (flying == null) {
          yield this;
        }
        yield new ChannelState(topic, channel, ready,
            added(inFlight, e.messageId(),
                new InFlight(flying.message(), flying.clientId(), e.deadline(), flying.deliveredAt())),
            deferred, clients, messageCount, requeueCount, timeoutCount, finishCount);
      }
      case ChannelEvent.ClientReadySet e -> {
        var existing = clients.get(e.clientId());
        var updated = existing == null
            ? new ClientState(e.rdy(), 0, e.msgTimeout(), e.at())
            : existing.withRdy(e.rdy(), e.msgTimeout(), e.at());
        yield new ChannelState(topic, channel, ready, inFlight, deferred,
            added(clients, e.clientId(), updated),
            messageCount, requeueCount, timeoutCount, finishCount);
      }
      case ChannelEvent.ClientForgotten e ->
          new ChannelState(topic, channel, ready, inFlight, deferred, removed(clients, e.clientId()),
              messageCount, requeueCount, timeoutCount, finishCount);
    };
  }

  // --- helpers ------------------------------------------------------------

  private ChannelState backToQueue(String messageId, long requeues, long timeouts) {
    var flying = inFlight.get(messageId);
    if (flying == null) {
      return this;
    }
    return new ChannelState(topic, channel, added(ready, messageId, flying.message()),
        removed(inFlight, messageId), deferred, adjust(flying.clientId(), ClientState::released),
        messageCount, requeues, timeouts, finishCount);
  }

  private ChannelState withReady(Map<String, Message> newReady) {
    return new ChannelState(topic, channel, newReady, inFlight, deferred, clients,
        messageCount, requeueCount, timeoutCount, finishCount);
  }

  private ChannelState withCounts(long messages, long requeues, long timeouts, long finishes) {
    return new ChannelState(topic, channel, ready, inFlight, deferred, clients,
        messages, requeues, timeouts, finishes);
  }

  private boolean knows(String messageId) {
    return ready.containsKey(messageId)
        || inFlight.containsKey(messageId)
        || deferred.containsKey(messageId);
  }

  private Answer heldBy(String clientId, String messageId) {
    var flying = inFlight.get(messageId);
    if (flying == null) {
      return new Answer.Refused("message " + messageId + " is not in flight");
    }
    if (!flying.clientId().equals(clientId)) {
      return new Answer.Refused("message " + messageId + " is in flight to another consumer");
    }
    return new Answer.Ok(List.of());
  }

  private static Duration cappedTimeout(Duration wanted, Duration ceiling) {
    return wanted.compareTo(ceiling) > 0 ? ceiling : wanted;
  }

  private static Duration clamp(Duration delay, Duration ceiling) {
    if (delay.isNegative()) {
      return Duration.ZERO;
    }
    return delay.compareTo(ceiling) > 0 ? ceiling : delay;
  }

  private Map<String, ClientState> adjust(
      String clientId, java.util.function.UnaryOperator<ClientState> change) {
    var client = clients.get(clientId);
    if (client == null) {
      return clients;
    }
    return added(clients, clientId, change.apply(client));
  }

  /** Re-adding an existing key keeps its place; a new key goes to the end. */
  private static <V> Map<String, V> added(Map<String, V> map, String key, V value) {
    var out = new LinkedHashMap<>(map);
    out.put(key, value);
    return Collections.unmodifiableMap(out);
  }

  private static <V> Map<String, V> removed(Map<String, V> map, String key) {
    var out = new LinkedHashMap<>(map);
    out.remove(key);
    return Collections.unmodifiableMap(out);
  }
}
