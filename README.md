# Greenski

Greenski is a small WhatsApp linked-device daemon modeled after
[Blueski](https://github.com/looskis/blueski). It exposes the same loopback HTTP
and CLI shape for sending messages, receiving messages and delivery updates,
replaying a durable event journal, and forwarding signed webhooks.

It runs as a separate WhatsApp companion device. Pairing creates an encrypted
session in a local SQLite database; the phone does not need to remain online
after the initial link.

> [!WARNING]
> Greenski uses WhatsApp's unofficial Web/linked-device protocol through
> [`whatsapp-rust`](https://github.com/jlucaso1/whatsapp-rust). It is not
> affiliated with or supported by WhatsApp or Meta. Protocol changes can break
> it, and automated or bulk messaging may violate WhatsApp's terms or trigger
> account restrictions. Use a dedicated account while evaluating it.

## Requirements

- A current stable Rust toolchain
- A WhatsApp account that can add a linked device
- macOS for `install`/`uninstall` LaunchAgent commands; the foreground daemon
  and other CLI commands work on Unix-like systems

## Install and pair

Install the current release with Homebrew:

```sh
brew install looskis/tap/greenski
greenski pair
brew services start greenski
```

Or build from source:

```sh
cargo install --locked --path .
greenski pair
greenski install # optional: start automatically on macOS
```

`greenski pair` starts the daemon, renders a QR code, and waits until the link
is connected. Scan it from WhatsApp under **Settings → Linked Devices → Link a
Device**.

`greenski install` installs its own per-user LaunchAgent. Do not use it at the
same time as `brew services`; choose one supervisor. If pairing started a
standalone daemon before you enable the Homebrew service, stop it with
`greenski down` first.

## Use

```sh
greenski status
greenski up
greenski send --to "+14155551234" --text "hello"
greenski send "+14155551234" "positional form"
greenski events --since 0
greenski events --follow
greenski chats
greenski messages --from "+14155551234" --limit 50
greenski sync --from "+14155551234" --count 100
greenski down
```

If you installed the LaunchAgent, use `greenski uninstall` to stop and remove
it; its keep-alive policy will restart a daemon stopped with `greenski down`.

Phone numbers should be in international format. To target an existing group
or chat directly, use its WhatsApp JID through the HTTP API's `chat_id` field.

### HTTP API

Greenski listens only on `127.0.0.1` (port `8789` by default). It is not an
authenticated network service; do not expose it through a proxy without adding
authentication.

Send a message:

```http
POST /messages
Content-Type: application/json

{
  "to": "+14155551234",
  "text": "hello",
  "protocol": "whatsapp",
  "client_ref": "example-123"
}
```

Use exactly one of `to` or `chat_id`. A valid request returns `202 Accepted`
with a generated local `message_id`. Sending and subsequent receipts appear as
events.

Other endpoints:

- `GET /status` — daemon and WhatsApp connection state
- `GET /pairing` — ephemeral in-memory pairing state used by the CLI
- `GET /events?since=<cursor>&limit=<n>` — durable event journal
- `GET /events/stream?since=<cursor>` — replay followed by live NDJSON events
- `GET /chats?limit=<n>` — chats captured from history sync and live traffic
- `GET /messages/history?from=<number-or-jid>&limit=<n>&before=<unix-ms>` — stored messages
- `POST /history/sync` — request an older page from the primary phone

The main event names match Blueski: `message.queued`, `message.sent`,
`message.failed`, `message.received`, and `message.status`. Connection lifecycle
events are also journaled. Provider delivery states progress through `sent`,
`delivered`, `read`, and `played` when WhatsApp supplies those receipts.

### Webhooks

Set `webhook_url` in the generated config. Every journaled event is POSTed as
JSON with an `X-Greenski-Signature` header containing the lowercase hex
HMAC-SHA256 of the exact request body using `hmac_secret`.

Webhook delivery is best effort with three attempts. The durable journal is the
source of truth and can always be replayed with a cursor.

## Configuration and state

The first command creates `~/.config/greenski/config.toml`:

```toml
port = 8789
webhook_url = "https://example.com/greenski/events" # optional
hmac_secret = "generated-64-character-secret"
```

State lives beside it:

- `whatsapp.db` — linked-device identity, encryption keys, and protocol state
- `state.db` — local Turso/libSQL database containing chats, messages, events,
  inbound dedupe, and outbound correlation
- `daemon.pid` and `daemon.lock` — local process lifecycle
- `greenski.log` and `greenski.err.log` — background daemon logs

The directory is mode `0700` and sensitive files are mode `0600` on Unix.
Deleting `whatsapp.db` logs out the local linked device and requires pairing
again. Remove the linked device from WhatsApp as well if you are decommissioning
an installation.

## Reliability model

Incoming decrypted messages are committed transactionally to `state.db` before
Greenski allows the protocol library to acknowledge them to WhatsApp. Duplicate
redeliveries are suppressed by `(chat_id, provider_message_id)`. Event IDs are
monotonic cursors, and live streams recover from receiver lag by replaying the
journal.

The dependency is pinned to a specific `whatsapp-rust` commit because the
pre-ack durability hook is newer than its published `0.6.0` crate. Default SIMD
is disabled so Greenski builds on stable Rust.

### Message history

Greenski consumes the initial and incremental history-sync chunks WhatsApp
sends to a linked device and streams each conversation into local libSQL. Text
content and metadata are queryable through `greenski chats` and `greenski
messages`; unsupported or media bodies are retained as typed message rows
without downloading their attachments.

`greenski sync --from <number-or-jid>` asks the primary phone for messages older
than the oldest locally stored anchor. WhatsApp controls how much history is
available and may enforce cooldowns or limits. A session paired before history
support was installed may need to be unlinked and paired once more to receive
its initial chat catalog; Greenski cannot reconstruct a history-sync blob it
previously acknowledged and discarded.

## Blueski parity

Greenski matches Blueski's public send/events/status workflow, loopback API,
automatic daemon start, durable cursor journal, outbound correlation,
delivery-state events, signed webhooks, CLI lifecycle, and macOS LaunchAgent.
WhatsApp-specific pairing and connection events replace Blueski's macOS
permission setup. Current scope stores live and historical text plus metadata;
attachment downloads, reactions, presence, and group administration are not
yet part of the Blueski-compatible surface.

## Development

```sh
cargo fmt --check
cargo test --locked
cargo clippy --all-targets -- -D warnings
```

For a real outbound check, copy `.env.example` to `.env`, set
`TEST_RECIPIENT`, pair the daemon, and run `scripts/smoke.sh`. This sends a real
WhatsApp message.

## Homebrew release checklist

The canonical formula lives in `looskis/homebrew-tap` and includes stable and
head builds plus a `brew services` definition. For each new version:

1. Tag `vX.Y.Z`; the release workflow uploads a deterministic source archive
   and SHA-256 file.
2. Update the release asset URL and SHA-256 in
   `looskis/homebrew-tap/Formula/greenski.rb`.
3. Run `brew audit --strict --online greenski`,
   `brew install --build-from-source greenski`, and `brew test greenski`.

## License

MIT
