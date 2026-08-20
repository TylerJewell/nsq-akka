package io.akka.nsq.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.nsq.domain.ChannelEvent;
import io.akka.nsq.domain.ChannelState;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The entity around the rules: that the id splits into a topic and a channel, that the
 * decisions of {@link ChannelState} are the events actually persisted, and that a
 * consumer's answer for a message it does not hold is refused rather than persisted.
 */
class ChannelEntityTest {

  static EventSourcedTestKit<ChannelState, ChannelEvent, ChannelEntity> channel() {
    return EventSourcedTestKit.of("orders:workers", ChannelEntity::new);
  }

  static void subscribe(
      EventSourcedTestKit<ChannelState, ChannelEvent, ChannelEntity> kit, String clientId, int rdy) {
    kit.method(ChannelEntity::setReady)
        .invoke(new ChannelEntity.Ready(clientId, rdy, Duration.ofSeconds(60)));
  }

  @Test
  void theEntityIdNamesTheTopicAndTheChannel() {
    var kit = channel();
    var stats = kit.method(ChannelEntity::stats).invoke().getReply();
    assertThat(stats.topic()).isEqualTo("orders");
    assertThat(stats.channel()).isEqualTo("workers");
  }

  @Test
  void aQueuedMessageIsHandedOutOnceAndCountsAnAttempt() {
    var kit = channel();
    subscribe(kit, "c1", 5);
    kit.method(ChannelEntity::queue)
        .invoke(new ChannelEntity.Incoming("m1", "work", Instant.now()));

    var delivered = kit.method(ChannelEntity::dispatch).invoke("c1").getReply();
    assertThat(delivered).hasSize(1);
    assertThat(delivered.get(0).body()).isEqualTo("work");
    assertThat(delivered.get(0).attempts()).isEqualTo(1);

    assertThat(kit.method(ChannelEntity::dispatch).invoke("c1").getReply())
        .as("nothing left waiting")
        .isEmpty();
  }

  @Test
  void theSameMessageArrivingTwiceIsQueuedOnce() {
    var kit = channel();
    var incoming = new ChannelEntity.Incoming("m1", "work", Instant.now());
    kit.method(ChannelEntity::queue).invoke(incoming);
    var second = kit.method(ChannelEntity::queue).invoke(incoming);

    assertThat(second.getAllEvents()).as("at-least-once fan-out, exactly-once queueing").isEmpty();
    assertThat(kit.getState().depth()).isEqualTo(1);
  }

  @Test
  void answeringForAMessageAnotherConsumerHoldsIsRefused() {
    var kit = channel();
    subscribe(kit, "c1", 1);
    subscribe(kit, "c2", 1);
    kit.method(ChannelEntity::queue)
        .invoke(new ChannelEntity.Incoming("m1", "work", Instant.now()));
    kit.method(ChannelEntity::dispatch).invoke("c1");

    var refused = kit.method(ChannelEntity::finish).invoke(new ChannelEntity.Held("c2", "m1"));
    assertThat(refused.isError()).isTrue();
    assertThat(kit.getState().inFlight()).containsKey("m1");

    kit.method(ChannelEntity::finish).invoke(new ChannelEntity.Held("c1", "m1"));
    assertThat(kit.getState().inFlight()).isEmpty();
  }

  @Test
  void aChannelWithNothingLeftToDoAsksNotToBeSweptAgain() {
    var kit = channel();
    assertThat(kit.method(ChannelEntity::sweep).invoke().getReply())
        .as("an untouched channel has no clock to run")
        .isFalse();

    subscribe(kit, "c1", 1);
    assertThat(kit.method(ChannelEntity::sweep).invoke().getReply())
        .as("a consumer is itself something a later sweep can act on")
        .isTrue();

    kit.method(ChannelEntity::forget).invoke("c1");
    assertThat(kit.method(ChannelEntity::sweep).invoke().getReply()).isFalse();
  }

  @Test
  void aSweepWithNothingDueChangesNothing() {
    var kit = channel();
    subscribe(kit, "c1", 1);
    kit.method(ChannelEntity::queue)
        .invoke(new ChannelEntity.Incoming("m1", "work", Instant.now()));
    kit.method(ChannelEntity::dispatch).invoke("c1");

    var swept = kit.method(ChannelEntity::sweep).invoke();
    assertThat(swept.getAllEvents()).isEmpty();
    assertThat(kit.getState().inFlight()).containsKey("m1");
  }
}
