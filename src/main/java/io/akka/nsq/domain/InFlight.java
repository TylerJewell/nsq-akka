package io.akka.nsq.domain;

import java.time.Instant;

/**
 * A message handed to a consumer and not yet answered for.
 *
 * @param deliveredAt when this delivery started; the ceiling on deadline extension is
 *     measured from here, so it resets on every redelivery (rule 10)
 */
public record InFlight(Message message, String clientId, Instant deadline, Instant deliveredAt) {}
