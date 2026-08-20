package io.akka.nsq.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.nsq.domain.Topic;
import io.akka.nsq.domain.TopicEvent;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** A topic: which channels are subscribed to it, and what has been published to it. */
@Component(id = "topic")
public class TopicEntity extends EventSourcedEntity<Topic, TopicEvent> {

  private final String name;

  public TopicEntity(EventSourcedEntityContext context) {
    this.name = context.entityId();
  }

  @Override
  public Topic emptyState() {
    return Topic.of(name);
  }

  public record Published(String messageId, List<String> channels) {}

  public Effect<Done> registerChannel(String channel) {
    if (currentState().channels().contains(channel)) {
      return effects().reply(Done.getInstance());
    }
    return effects()
        .persist(new TopicEvent.ChannelRegistered(channel))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Published> publish(String body) {
    var event = new TopicEvent.MessagePublished(
        UUID.randomUUID().toString(), body, Instant.now(), currentState().channelList());
    return effects()
        .persist(event)
        .thenReply(s -> new Published(event.messageId(), event.channels()));
  }

  public ReadOnlyEffect<Topic> get() {
    return effects().reply(currentState());
  }

  @Override
  public Topic applyEvent(TopicEvent event) {
    return switch (event) {
      case TopicEvent.ChannelRegistered e -> currentState().withChannel(e.channel());
      case TopicEvent.MessagePublished ignored -> currentState().published();
    };
  }
}
