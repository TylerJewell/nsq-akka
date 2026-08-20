# Acknowledgements

## nsqio/nsq

This project reimplements part of the behaviour of [nsq](https://github.com/nsqio/nsq),
a realtime distributed messaging platform by Bitly and its contributors, released under
the MIT licence.

Specifically, it reimplements what nsq calls the in-flight lifecycle of a message on a
channel — dispatch under a ready count, finish, requeue, deferred requeue, touch, and the
in-flight timeout — together with the consumer-side backoff and attempt ceiling that live
in nsq's Go client library, [go-nsq](https://github.com/nsqio/go-nsq), also MIT licensed.

**No source was copied.** The behaviour was established by running nsqd and a real go-nsq
consumer and recording what they did (41 checks, in
`akka-specify-harness/nsq-port/probes/`), written up as a specification, and then written
fresh in Java against the Akka SDK. Where this port's behaviour departs from nsq's, the
departure is listed in `README.md` under "Where it differs from nsq".

Names shared with nsq — topic, channel, ready count, in-flight, requeue, touch, attempts —
are kept deliberately, so that someone who knows nsq can read this without a glossary.

nsq's licence, in full, is at https://github.com/nsqio/nsq/blob/master/LICENSE.

## Akka

Built on the [Akka SDK](https://doc.akka.io/), © Lightbend Inc., under the Business Source
Licence.
