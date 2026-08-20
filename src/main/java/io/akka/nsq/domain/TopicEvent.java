package io.akka.nsq.domain;

import akka.javasdk.annotations.TypeName;
import java.time.Instant;
import java.util.List;

/** What can happen to a topic. */
public sealed interface TopicEvent {

  @TypeName("channel-registered")
  record ChannelRegistered(String channel) implements TopicEvent {}

  /**
   * The channels a message is destined for are recorded on the event rather than looked
   * up when it is delivered, so the fan-out cannot pick up channels created after the
   * publication and cannot miss ones deleted before it.
   */
  @TypeName("message-published")
  record MessagePublished(String messageId, String body, Instant publishedAt, List<String> channels)
      implements TopicEvent {}
}
