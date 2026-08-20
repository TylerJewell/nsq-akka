# nsq-akka

Hands each published message to a consumer, and keeps handing it back until the consumer
says it is done.

A port of [nsqio/nsq](https://github.com/nsqio/nsq) onto **Akka**, built with **Akka Specify**.

---

## Where it came from

nsq is a message queue: producers publish to a named subject, consumers subscribe to it,
and the queue takes responsibility for getting every message to every consumer at least
once. It was ported to derive a specification format precise enough to regenerate a system
on a different stack — the port is the vehicle, the specification is the deliverable.

This port takes one part of nsq: what happens to a single message between being handed to
a consumer and being finished with. That is the part that has to be right for the queue to
mean anything, and it is split across two programs in nsq — the server decides when to
hand a message back, and the consumer library decides how fast to ask for more.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `nsq-port/`.

---

## nsqio/nsq → this port

📉 1,218 Go lines → **901 Java lines**<br>
📁 11 files → **17 files**<br>
🖥️ 2 programs → **1 program**<br>
✅ 16 behaviours compared → **16 of 16 agree**<br>
⚡ 3.93 → **12.22** milliseconds, one publish<br>
⏱️ 4,095 → **175** milliseconds late, worst redelivery past a 1-second deadline<br>
🧪 1,930 test lines → **691 test lines**<br>
🔨 0 rules broken on purpose to check a test notices → **17**

Full method and the numbers that did *not* make this list: [`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/nsq-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.3 hours** from the first command to the published repository, **1.3** of them active<br>
💬 **266** exchanges with the model<br>
✍️ **366,337** tokens written by the model, **62,226,324** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **54** tests

```bash
python toolkit/tokens.py --port nsq    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

From the specification:

- **A message handed to a consumer is owed an answer.** If the consumer does not say
  whether it finished, the message comes back and is handed to someone else.
- **A consumer says how much it will hold, and is never given more.** Setting that number
  to zero stops work arriving; raising it releases exactly that many messages.
- **Lowering that number does not take back what has already been handed out.** A message
  already given to a consumer is still that consumer's to answer for.
- **A message put back goes behind everything already waiting**, either at once or after a
  delay the consumer asks for.
- **A consumer that needs longer can say so**, and keep saying so, up to a limit measured
  from when it was given the message.
- **Only the consumer holding a message may answer for it.** Anyone else is refused, and
  the message stays where it was.
- **The queue never gives up on a message.** Deciding a message is not worth another try
  is the consumer's decision, and the queue is never told that is what happened.
- **A consumer that keeps failing is slowed down, not switched off.** After each failure
  it waits, then asks for exactly one message to find out whether the trouble has passed,
  and returns to full speed only after as many successes as there were failures.

---

## Design decisions

**One sweep per subject, not one alarm per message.** Setting an alarm for every message
in flight would let this run out of alarms — the platform allows fifty thousand — while
one repeating check per subject costs the same whether it is watching one message or a
million. It also means a subject created a second ago is checked on the same schedule as
one created a year ago.

**The queue and the consumer keep separate books.** The queue knows where each message is
and who has it; it does not know why a consumer slowed down, and the consumer does not
know where anything is. Either half can be replaced without touching the other, and no
consumer's bad day can corrupt what the queue believes.

**A message is a copy per subscriber, not a shared one.** Two consumer groups reading the
same subject each get their own copy with its own count of how many times it has been
tried. One group failing over and over changes nothing for the other.

**Messages are handed out on request, not pushed down a held-open line.** The rule being
copied is how many a consumer may hold and what happens when it stays silent, and that is
about counting rather than about who speaks first. Asking for work keeps the counting
exactly and removes a whole layer of connection bookkeeping.

**Asking for a message twice is the same as asking once.** Copying a published message to
each subscriber can be retried after a failure, so the second copy of the same message is
recognised and ignored. Nothing has to be exactly-once for the count of tries to stay
honest.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/nsq-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Try it** at http://localhost:9015.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9015**.

### Try it

```bash
# subscribe: a consumer called "worker" joins subject "orders", group "billing",
# willing to hold 2 messages at once, with 30 seconds to answer for each
curl -X POST localhost:9015/nsq/topics/orders/channels/billing/consumers/worker \
  -H 'Content-Type: application/json' -d '{"rdy":2,"msgTimeoutMillis":30000}'

# publish
curl -X POST localhost:9015/nsq/topics/orders/messages \
  -H 'Content-Type: application/json' -d '{"body":"invoice-1"}'

# take work
curl -X POST localhost:9015/nsq/topics/orders/channels/billing/consumers/worker/messages

# say the handler failed; the reply says what to do and how much to ask for next
curl -X POST localhost:9015/nsq/topics/orders/channels/billing/consumers/worker/outcomes \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"<id from above>","outcome":"FAILURE","attempts":1}'

# counts
curl localhost:9015/nsq/topics/orders/channels/billing/stats
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `HTTP_PORT` | `9015` | set in `src/main/resources/application.conf`, not by an environment variable |

The limits below are constants in the code rather than settings, and changing one means
editing `ChannelState`. They match nsq's own defaults.

| Limit | Value | What it does |
|---|---|---|
| Time to answer for a message | 60 seconds | used when a consumer names none of its own |
| Longest a consumer can extend that to | 15 minutes | measured from the delivery being extended |
| Longest delay a consumer can ask for | 1 hour | a longer request is cut down to this, not refused |
| How often each subject is checked | 250 milliseconds | the most a message can be handed back late by |
| How long a silent consumer is remembered | 10 minutes | only if it is holding nothing |

---

## Where it differs from nsq

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **How late a message can come back.** nsq checks messages in flight from a list of
  subjects it rebuilds every five seconds, so a subject created since the last rebuild is
  not checked at all until the next one: over five runs a one-second deadline was measured
  coming back between 3.98 and 4.10 seconds late. This port gives each subject its own
  check every 250 milliseconds from the moment it exists, and the same measurement is
  between 0.11 and 0.17 seconds late, because a rule nobody can put a ceiling on is not a
  rule a caller can build on. nsq promises no ceiling
  and so is not doing anything it said it would not.
- **What order messages arrive in.** nsq's own documentation says messages arrive in no
  particular order, and a single consumer draining a single subject with nothing put back
  nevertheless receives them in the order they were published. This port promises that
  order, so a reader can tell whether they have it, and promises that a message put back
  goes behind everything waiting.
- **How a consumer gets a message.** nsq pushes messages down a connection the consumer
  holds open. Here a consumer asks, and is handed at most what its stated limit still
  allows. The count of what it holds works out the same, and a consumer that stops asking
  is treated exactly like one that stops answering.
- **How a consumer is noticed to have gone.** nsq drops a consumer when its connection
  drops. Nothing is held open here, so a consumer that has said nothing for ten minutes
  and is holding nothing is forgotten instead. Either way, a message a departed consumer
  was holding is not released early: it waits out its deadline.
- **Copying a published message to each subscriber takes a moment.** In nsq a publish and
  the copies it produces happen in one step inside one program. Here the copying is a
  separate step that follows, so a message can be published and not yet be waiting for a
  consumer a few milliseconds later. Nothing is lost — the step is retried until it
  succeeds — but a test that publishes and immediately asks for work has to wait.
- **Where messages wait when there are a lot of them.** nsq keeps a fixed number in memory
  and writes the rest to disk. This port keeps them all in the subject's own stored state,
  with nothing that spills or trims. Behaviour under a backlog large enough to matter is
  **not checked** on either side.
- **Dropping a fraction of messages on purpose.** nsq can be told to sample: to discard a
  proportion of messages before they are ever handed out. This port has nothing
  corresponding to it, because every rule it copies is a promise that nothing is lost.
- **Finding servers, and spreading a consumer's limit across several of them.** nsq has a
  discovery service and consumer logic that shares one limit between connections to many
  servers. This port has one service and no discovery, so there is nothing to spread.
- **The number of times a message may be tried before a consumer gives up.** Both systems
  put this decision in the consumer rather than the queue, and both default to five. In
  nsq it is a setting inside the consumer program; here it is a setting on the consumer's
  record, changed through the service.
- **What a subject does with a message when nobody is subscribed to it.** In this port the
  message is recorded as published and copied to nobody, so it is not delivered later to a
  consumer that subscribes afterwards. Whether nsq does the same is **not checked**.
- **Everything below the delivery rules.** Encryption in transit, compression, the binary
  wire format, and the command-line tools that ship with nsq are not here. These are not
  differences in behaviour so much as parts that were never attempted.

---

## Licence

nsq is MIT licensed, © Bitly Inc. and the nsq authors. This port reimplements the
behaviour without copied source; see [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
