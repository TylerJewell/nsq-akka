package io.akka.nsq.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.timer.TimerScheduler;
import io.akka.nsq.application.ChannelEntity;
import io.akka.nsq.application.ChannelSweeper;
import io.akka.nsq.application.Channels;
import io.akka.nsq.application.ConsumerSessionEntity;
import io.akka.nsq.application.TopicEntity;
import io.akka.nsq.domain.ChannelState;
import io.akka.nsq.domain.ConsumerSession;
import java.time.Duration;
import java.util.List;

/**
 * The whole surface: publish, subscribe, take work, answer for it.
 *
 * <p>nsq pushes messages down a long-lived TCP connection and spends RDY as a credit;
 * here a consumer asks and is handed at most what its ready count still allows. What is
 * being ported is the accounting, not the framing — see SPEC-001 §4 decision 1.
 *
 * <p>Opened up for access from the public internet to make this port easy to try out; a
 * production service would scope this more tightly.
 */
@HttpEndpoint("/nsq")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class NsqEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;
  private final TimerScheduler timerScheduler;

  public NsqEndpoint(ComponentClient componentClient, TimerScheduler timerScheduler) {
    this.componentClient = componentClient;
    this.timerScheduler = timerScheduler;
  }

  public record PublishBody(String body) {}

  public record PublishedMessage(String messageId, List<String> channels) {}

  public record SubscribeBody(int rdy, Long msgTimeoutMillis) {}

  public record RequeueBody(Long delayMillis) {}

  public record OutcomeBody(String messageId, String outcome, int attempts) {}

  public record OutcomeResult(String action, long requeueDelayMillis, int rdy) {}

  /** One message handed to a consumer, and the deadline it has to answer by. */
  public record MessageView(String messageId, String body, int attempts, String deadline) {
    static MessageView of(ChannelEntity.Delivery delivery) {
      return new MessageView(delivery.messageId(), delivery.body(), delivery.attempts(),
          delivery.deadline().toString());
    }
  }

  /** The counters nsqadmin's channel screen shows, over this port's channel. */
  public record StatsView(
      String topic,
      String channel,
      int depth,
      int inFlight,
      int deferred,
      long messageCount,
      long requeueCount,
      long timeoutCount,
      long finishCount) {

    static StatsView of(ChannelEntity.Stats stats) {
      return new StatsView(stats.topic(), stats.channel(), stats.depth(), stats.inFlight(),
          stats.deferred(), stats.messageCount(), stats.requeueCount(), stats.timeoutCount(),
          stats.finishCount());
    }
  }

  // --- producing ----------------------------------------------------------

  @Post("/topics/{topic}/messages")
  public PublishedMessage publish(String topic, PublishBody body) {
    var published = componentClient
        .forEventSourcedEntity(topic)
        .method(TopicEntity::publish)
        .invoke(body.body());
    return new PublishedMessage(published.messageId(), published.channels());
  }

  // --- consuming ----------------------------------------------------------

  /**
   * Join a channel and declare a ready count. Both the topic and the channel come into
   * being here if they did not exist, as they do in nsq, and the channel's sweep starts
   * with it.
   */
  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}")
  public int subscribe(String topic, String channel, String clientId, SubscribeBody body) {
    componentClient.forEventSourcedEntity(topic)
        .method(TopicEntity::registerChannel).invoke(channel);

    var channelId = Channels.id(topic, channel);
    var msgTimeout = body.msgTimeoutMillis() == null
        ? ChannelState.DEFAULT_MSG_TIMEOUT
        : Duration.ofMillis(body.msgTimeoutMillis());
    componentClient.forEventSourcedEntity(channelId)
        .method(ChannelEntity::setReady)
        .invoke(new ChannelEntity.Ready(clientId, body.rdy(), msgTimeout));

    ChannelSweeper.arm(timerScheduler, componentClient, channelId);
    return body.rdy();
  }

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/ready/{rdy}")
  public int setReady(String topic, String channel, String clientId, int rdy) {
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::setReady)
        .invoke(new ChannelEntity.Ready(clientId, rdy, ChannelState.DEFAULT_MSG_TIMEOUT));
    return rdy;
  }

  /** Take as much work as the ready count still allows. */
  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/messages")
  public List<MessageView> take(String topic, String channel, String clientId) {
    return componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::dispatch)
        .invoke(clientId)
        .stream()
        .map(MessageView::of)
        .toList();
  }

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/messages/{messageId}/finish")
  public String finish(String topic, String channel, String clientId, String messageId) {
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::finish)
        .invoke(new ChannelEntity.Held(clientId, messageId));
    return "finished";
  }

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/messages/{messageId}/requeue")
  public String requeue(
      String topic, String channel, String clientId, String messageId, RequeueBody body) {
    long delay = body == null || body.delayMillis() == null ? 0 : body.delayMillis();
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::requeue)
        .invoke(new ChannelEntity.Requeue(clientId, messageId, delay));
    return "requeued";
  }

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/messages/{messageId}/touch")
  public String touch(String topic, String channel, String clientId, String messageId) {
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::touch)
        .invoke(new ChannelEntity.Held(clientId, messageId));
    return "touched";
  }

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/leave")
  public String leave(String topic, String channel, String clientId) {
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::forget)
        .invoke(clientId);
    return "left";
  }

  // --- the consumer's own half -------------------------------------------

  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/settings")
  public int configure(
      String topic, String channel, String clientId, ConsumerSessionEntity.Settings settings) {
    return componentClient
        .forKeyValueEntity(Channels.sessionId(topic, channel, clientId))
        .method(ConsumerSessionEntity::configure)
        .invoke(settings);
  }

  /**
   * Report what the handler did with a message. The session decides whether the message
   * is finished, put back, or given up on, and what the ready count becomes — and then
   * this carries that decision out, so a consumer needs one call per message rather than
   * three.
   */
  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/outcomes")
  public OutcomeResult report(String topic, String channel, String clientId, OutcomeBody body) {
    var outcome = ConsumerSession.Outcome.valueOf(body.outcome().toUpperCase());
    var decision = componentClient
        .forKeyValueEntity(Channels.sessionId(topic, channel, clientId))
        .method(ConsumerSessionEntity::report)
        .invoke(new ConsumerSessionEntity.Report(outcome, body.attempts()));

    var channelId = Channels.id(topic, channel);
    switch (decision.action()) {
      case FINISH, GIVE_UP -> componentClient.forEventSourcedEntity(channelId)
          .method(ChannelEntity::finish)
          .invoke(new ChannelEntity.Held(clientId, body.messageId()));
      case REQUEUE -> componentClient.forEventSourcedEntity(channelId)
          .method(ChannelEntity::requeue)
          .invoke(new ChannelEntity.Requeue(clientId, body.messageId(), decision.requeueDelayMillis()));
    }
    componentClient.forEventSourcedEntity(channelId)
        .method(ChannelEntity::setReady)
        .invoke(new ChannelEntity.Ready(clientId, decision.rdy(), ChannelState.DEFAULT_MSG_TIMEOUT));

    return new OutcomeResult(
        decision.action().name(), decision.requeueDelayMillis(), decision.rdy());
  }

  /**
   * Bring the ready count up to date. A backoff wait expires on the clock, so the count a
   * consumer should be at changes without anything happening to a message.
   */
  @Post("/topics/{topic}/channels/{channel}/consumers/{clientId}/refresh")
  public int refreshReady(String topic, String channel, String clientId) {
    int rdy = componentClient
        .forKeyValueEntity(Channels.sessionId(topic, channel, clientId))
        .method(ConsumerSessionEntity::readyCount)
        .invoke();
    componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::setReady)
        .invoke(new ChannelEntity.Ready(clientId, rdy, ChannelState.DEFAULT_MSG_TIMEOUT));
    return rdy;
  }

  // --- looking ------------------------------------------------------------

  @Get("/topics/{topic}/channels/{channel}/stats")
  public StatsView stats(String topic, String channel) {
    return StatsView.of(componentClient.forEventSourcedEntity(Channels.id(topic, channel))
        .method(ChannelEntity::stats)
        .invoke());
  }
}
