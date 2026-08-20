package io.akka.nsq.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import akka.javasdk.timer.TimerScheduler;
import io.akka.nsq.domain.TopicEvent;

/**
 * Copies a published message onto every channel of its topic — SPEC-001 rule 4.
 *
 * <p>Delivery here is at-least-once, so this can run twice for the same message.
 * {@link ChannelEntity#queue} is idempotent on the message id for that reason, and the
 * duplicate produces no second copy.
 */
@Component(id = "topic-fan-out")
@Consume.FromEventSourcedEntity(value = TopicEntity.class)
public class TopicFanOutConsumer extends Consumer {

  private final ComponentClient componentClient;
  private final TimerScheduler timerScheduler;

  public TopicFanOutConsumer(ComponentClient componentClient, TimerScheduler timerScheduler) {
    this.componentClient = componentClient;
    this.timerScheduler = timerScheduler;
  }

  public Effect onEvent(TopicEvent event) {
    if (event instanceof TopicEvent.MessagePublished published) {
      for (var channel : published.channels()) {
        var channelId = Channels.id(topicName(), channel);
        componentClient
            .forEventSourcedEntity(channelId)
            .method(ChannelEntity::queue)
            .invoke(new ChannelEntity.Incoming(
                published.messageId(), published.body(), published.publishedAt()));
        // the sweep stops itself on an idle channel, so giving one work restarts it
        ChannelSweeper.arm(timerScheduler, componentClient, channelId);
      }
    }
    return effects().done();
  }

  private String topicName() {
    return messageContext().eventSubject().orElseThrow();
  }
}
