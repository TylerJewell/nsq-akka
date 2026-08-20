package io.akka.nsq.domain;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A topic is a name and the set of channels subscribed to it. Publishing to a topic with
 * no channels leaves the message nowhere to go, exactly as in nsq — this port keeps that
 * and says so in its README rather than quietly inventing a holding area.
 */
public record Topic(String name, Set<String> channels, long publishedCount) {

  public static Topic of(String name) {
    return new Topic(name, Set.of(), 0);
  }

  public Topic withChannel(String channel) {
    var out = new LinkedHashSet<>(channels);
    out.add(channel);
    return new Topic(name, Set.copyOf(out), publishedCount);
  }

  public Topic published() {
    return new Topic(name, channels, publishedCount + 1);
  }

  public List<String> channelList() {
    return List.copyOf(channels);
  }
}
