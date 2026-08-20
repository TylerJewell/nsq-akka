package io.akka.nsq;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.nsq.api.NsqEndpoint;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rules 4, 12, 24 and the whole HTTP surface, against a running runtime.
 *
 * <p>What is checked here and not in {@code ChannelStateTest} is everything a pure state
 * machine cannot answer: that the sweep fires at all, that it fires soon enough, and that
 * a published message reaches every channel through the fan-out rather than only in
 * principle.
 */
public class ChannelDeliveryIntegrationTest extends TestKitSupport {

  private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(1);

  private String topic() {
    return "topic-" + UUID.randomUUID();
  }

  private void subscribe(String topic, String channel, String clientId, int rdy, Duration timeout) {
    httpClient
        .POST("/nsq/topics/" + topic + "/channels/" + channel + "/consumers/" + clientId)
        .withRequestBody(new NsqEndpoint.SubscribeBody(rdy, timeout.toMillis()))
        .invoke();
  }

  private NsqEndpoint.PublishedMessage publish(String topic, String body) {
    return httpClient
        .POST("/nsq/topics/" + topic + "/messages")
        .withRequestBody(new NsqEndpoint.PublishBody(body))
        .responseBodyAs(NsqEndpoint.PublishedMessage.class)
        .invoke()
        .body();
  }

  private List<NsqEndpoint.MessageView> take(String topic, String channel, String clientId) {
    return httpClient
        .POST("/nsq/topics/" + topic + "/channels/" + channel + "/consumers/" + clientId + "/messages")
        .responseBodyAsListOf(NsqEndpoint.MessageView.class)
        .invoke()
        .body();
  }

  private NsqEndpoint.StatsView stats(String topic, String channel) {
    return httpClient
        .GET("/nsq/topics/" + topic + "/channels/" + channel + "/stats")
        .responseBodyAs(NsqEndpoint.StatsView.class)
        .invoke()
        .body();
  }

  private void report(String topic, String channel, String clientId, String messageId,
      String outcome, int attempts) {
    httpClient
        .POST("/nsq/topics/" + topic + "/channels/" + channel + "/consumers/" + clientId + "/outcomes")
        .withRequestBody(new NsqEndpoint.OutcomeBody(messageId, outcome, attempts))
        .invoke();
  }

  private List<NsqEndpoint.MessageView> awaitDelivery(String topic, String channel, String clientId) {
    var box = new java.util.ArrayList<NsqEndpoint.MessageView>();
    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          var taken = take(topic, channel, clientId);
          box.addAll(taken);
          assertThat(box).isNotEmpty();
        });
    return List.copyOf(box);
  }

  @Test
  void aPublishedMessageReachesEveryChannelOfItsTopic() {
    var topic = topic();
    subscribe(topic, "a", "c1", 5, SHORT_TIMEOUT);
    subscribe(topic, "b", "c1", 5, SHORT_TIMEOUT);
    var published = publish(topic, "hello");
    assertThat(published.channels()).containsExactlyInAnyOrder("a", "b");

    var onA = awaitDelivery(topic, "a", "c1");
    var onB = awaitDelivery(topic, "b", "c1");
    assertThat(onA.get(0).body()).isEqualTo("hello");
    assertThat(onB.get(0).body()).isEqualTo("hello");
    assertThat(onA.get(0).messageId()).isEqualTo(onB.get(0).messageId());
  }

  @Test
  void anUnansweredMessageComesBackWithoutTheConsumerAskingForIt() {
    var topic = topic();
    subscribe(topic, "work", "c1", 1, SHORT_TIMEOUT);
    publish(topic, "unanswered");

    var first = awaitDelivery(topic, "work", "c1");
    assertThat(first.get(0).attempts()).isEqualTo(1);

    // say nothing at all, and wait out the deadline
    var again = awaitDelivery(topic, "work", "c1");
    assertThat(again.get(0).messageId()).isEqualTo(first.get(0).messageId());
    assertThat(again.get(0).attempts()).isEqualTo(2);
    assertThat(stats(topic, "work").timeoutCount()).isEqualTo(1);
  }

  /**
   * Rule 24. The channel here is seconds old, which is exactly the case nsqd is late on:
   * its sweeper's channel list is rebuilt every five seconds, and a 1s deadline on a young
   * channel was measured firing at 4.58s (question-log #3).
   */
  @Test
  void aRedeliveryOnAChannelJustCreatedIsLateByAtMostOneSweep() {
    var topic = topic();
    subscribe(topic, "fresh", "c1", 1, SHORT_TIMEOUT);
    publish(topic, "fresh");

    var first = awaitDelivery(topic, "fresh", "c1");
    long dispatchedAt = System.currentTimeMillis();
    var again = awaitDelivery(topic, "fresh", "c1");
    long late = System.currentTimeMillis() - dispatchedAt;

    assertThat(again.get(0).messageId()).isEqualTo(first.get(0).messageId());
    assertThat(late)
        .as("1s deadline plus at most one 250ms sweep, plus the time to notice")
        .isLessThan(2500L);
  }

  @Test
  void aDeferredRequeueComesBackAfterItsDelayAndNotBefore() {
    var topic = topic();
    subscribe(topic, "work", "c1", 1, Duration.ofMinutes(5));
    publish(topic, "deferred");
    var first = awaitDelivery(topic, "work", "c1");

    httpClient
        .POST("/nsq/topics/" + topic + "/channels/work/consumers/c1/messages/"
            + first.get(0).messageId() + "/requeue")
        .withRequestBody(new NsqEndpoint.RequeueBody(1500L))
        .invoke();

    assertThat(stats(topic, "work").deferred()).isEqualTo(1);
    assertThat(take(topic, "work", "c1")).as("still deferred").isEmpty();

    var again = awaitDelivery(topic, "work", "c1");
    assertThat(again.get(0).attempts()).isEqualTo(2);
    assertThat(stats(topic, "work").requeueCount()).isEqualTo(1);
  }

  /** Rules 16, 17, 23 driven end to end: a failing handler slows down, then gives up. */
  @Test
  void aFailingConsumerBacksOffAndEventuallyGivesUp() {
    var topic = topic();
    subscribe(topic, "work", "c1", 3, Duration.ofMinutes(5));
    httpClient
        .POST("/nsq/topics/" + topic + "/channels/work/consumers/c1/settings")
        .withRequestBody(new io.akka.nsq.application.ConsumerSessionEntity.Settings(
            3, 3, 50, 200, 10, 100))
        .invoke();
    publish(topic, "poison");

    String messageId = null;
    int attempts = 0;
    for (int i = 0; i < 10; i++) {
      var delivered = awaitDelivery(topic, "work", "c1");
      messageId = delivered.get(0).messageId();
      attempts = delivered.get(0).attempts();
      report(topic, "work", "c1", messageId, "FAILURE", attempts);
      if (attempts > 3) {
        break;
      }
      // the consumer asked for nothing while backing off; let the wait expire
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> httpClient
              .POST("/nsq/topics/" + topic + "/channels/work/consumers/c1/refresh")
              .responseBodyAs(Integer.class)
              .invoke()
              .body() > 0);
    }

    assertThat(attempts).as("gave up rather than requeuing forever").isEqualTo(4);
    var after = stats(topic, "work");
    assertThat(after.depth()).isZero();
    assertThat(after.inFlight()).isZero();
    assertThat(after.deferred()).isZero();
    assertThat(after.finishCount()).as("given up on, which the channel sees as finished").isEqualTo(1);
  }
}
