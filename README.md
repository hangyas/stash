# 🗄 STASH

A 🚧 work-in-progress 🚧 key-value storage optimized for scripting

Stash is a tiny, zero dependency, embeddable key-value engine that trades enterprise features for **zero-config speed** inside shell scripts, Python glue, CI jobs, or anywhere a JSON file feels too clunky and Redis feels like overkill. Perfect for config blobs, session tokens, or ad-hoc data without firing up a full database. Think of it as persistent collections for your scripts.

Goals of the project:

- **Single-file executable** – drop it in `$PATH`, no installer.
- **Fast startup** - no need to set up connections in your script, you can just call the cli
- **Human-first CLI** – `stash set api_key 1234`, `stash get api_key`, `stash ls | grep prod`.
- **Language bindings** – `import stash` in Python, `require 'stash'` in Ruby, or pipe JSON via stdin/stdout.
- **Optimised for 1 kB–1 MB values** – perfect for configs, counters, cached JWTs, not for your photo archive.
- **Basic data structures** - values can be stored in hash maps, lists, queues and stacks

Non-goals:
- **High throughput** - optimized for single user
- **Clustering** - data replication might be added later, but similar to sqlite, stash is more like a file than a network endpoint
- **Complex queries**

## Development

Written in zig with 0 dependencies to match the performance goals

```
zig build
```

```
zig test ./src/node_builder.zig
```

### Binary format

header:
```
| kind(1) | key_count(2) | child_pointers((key_count + 1) * 8) | offsets(key_count * 2) | kv pairs... |
```

kv pair:
```
| key_length(8) | value_length(8) | key(key_length * 8) | value(value_length * 8) |
```

## Status

**alpha**, interfaces may change, but the data format is already forward-compatible.
