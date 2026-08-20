package io.akka.nsq.application;

/** How a channel's entity id is spelled, in one place. */
public final class Channels {

  private Channels() {}

  public static String id(String topic, String channel) {
    return topic + ":" + channel;
  }

  public static String sessionId(String topic, String channel, String clientId) {
    return topic + ":" + channel + ":" + clientId;
  }

  public static String sweepTimer(String topic, String channel) {
    return "sweep-" + id(topic, channel);
  }
}
