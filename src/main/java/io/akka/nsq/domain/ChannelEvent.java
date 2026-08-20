package io.akka.nsq.domain;

import akka.javasdk.annotations.TypeName;
import java.time.Duration;
import java.time.Instant;

/** Everything that can happen to a channel's delivery state. */
public sealed interface ChannelEvent {

  @TypeName("message-queued")
  record MessageQueued(String messageId, String body, Instant publishedAt) implements ChannelEvent {}

  @TypeName("message-dispatched")
  record MessageDispatched(
      String messageId, String clientId, int attempts, Instant deadline, Instant deliveredAt)
      implements ChannelEvent {}

  @TypeName("message-finished")
  record MessageFinished(String messageId) implements ChannelEvent {}

  /** In flight, then put back at the end of the ready queue. */
  @TypeName("message-requeued")
  record MessageRequeued(String messageId) implements ChannelEvent {}

  /** In flight, then held out of the queue until {@code readyAt}. */
  @TypeName("message-deferred")
  record MessageDeferred(String messageId, Instant readyAt) implements ChannelEvent {}

  /** The deadline passed with no answer, so the message goes back to the queue. */
  @TypeName("message-timed-out")
  record MessageTimedOut(String messageId) implements ChannelEvent {}

  @TypeName("deferred-released")
  record DeferredReleased(String messageId) implements ChannelEvent {}

  @TypeName("deadline-extended")
  record DeadlineExtended(String messageId, Instant deadline) implements ChannelEvent {}

  @TypeName("client-ready-set")
  record ClientReadySet(String clientId, int rdy, Duration msgTimeout, Instant at)
      implements ChannelEvent {}

  @TypeName("client-forgotten")
  record ClientForgotten(String clientId) implements ChannelEvent {}
}
