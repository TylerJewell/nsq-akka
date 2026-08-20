package io.akka.nsq.domain;

import java.time.Instant;

/** A message held out of the ready queue until {@code readyAt}. */
public record DeferredMessage(Message message, Instant readyAt) {}
