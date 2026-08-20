package io.akka.nsq.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.timedaction.TimedAction;
import akka.javasdk.timer.TimerScheduler;
import java.time.Duration;

/**
 * The clock behind the in-flight timeout — SPEC-001 rule 24, §4 decision 2.
 *
 * <p>One repeating sweep per channel, re-armed by each run. nsqd does the same thing with
 * a shared worker pool over a list of channels rebuilt every few seconds; a per-channel
 * timer removes the part of that which makes a young channel behave differently from an
 * old one, and bounds how late a redelivery can be by {@link #INTERVAL}.
 *
 * <p>A timer per in-flight message would give a sharper bound, and is not used: the
 * platform allows 50,000 active timers per service, which would cap the whole service at
 * 50,000 concurrently in-flight messages (question-log #30).
 */
@Component(id = "channel-sweeper")
public class ChannelSweeper extends TimedAction {

  public static final Duration INTERVAL = Duration.ofMillis(250);

  private final ComponentClient componentClient;
  private final TimerScheduler timerScheduler;

  public ChannelSweeper(ComponentClient componentClient, TimerScheduler timerScheduler) {
    this.componentClient = componentClient;
    this.timerScheduler = timerScheduler;
  }

  public Effect sweep(String channelId) {
    boolean stillHasWork = componentClient
        .forEventSourcedEntity(channelId)
        .method(ChannelEntity::sweep)
        .invoke();
    if (stillHasWork) {
      arm(timerScheduler, componentClient, channelId);
    }
    return effects().done();
  }

  /**
   * Start, or restart, a channel's sweep. Creating a timer that already exists replaces
   * it, so a channel that is subscribed to repeatedly still has exactly one sweep — and
   * anything that gives a channel work to do may call this without checking whether the
   * sweep is already running.
   */
  public static void arm(
      TimerScheduler timerScheduler, ComponentClient componentClient, String channelId) {
    timerScheduler.createSingleTimer(
        "sweep-" + channelId,
        INTERVAL,
        componentClient.forTimedAction().method(ChannelSweeper::sweep).deferred(channelId));
  }
}
