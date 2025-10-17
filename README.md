# kt - a Kafka tool that likes JSON [![Continuous Integration](https://github.com/fujiwara/kt/actions/workflows/go.yml/badge.svg)](https://github.com/fujiwara/kt/actions/workflows/go.yml)

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Time-based offset consumption: Start consuming from specific timestamps (RFC3339), relative times (e.g., `-1h`, `+30m`), or current time (`now`).
* Display topic information (e.g., with partition offset and leader info).
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition).
* Built-in jq integration: Apply [jq](https://stedolan.github.io/jq/) filters directly to output using [gojq](https://github.com/itchyny/gojq) without external piping, with support for raw string output.
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers, topic, Kafka version and authentication via environment variables `KT_BROKERS`, `KT_TOPIC`, `KT_KAFKA_VERSION`, `KT_AUTH`, and `KT_ADMIN_TIMEOUT`.
* Fast start up time.
* Adaptive output buffering: unbuffered for terminals, buffered for pipes and files to improve performance.
* Binary keys and payloads can be passed and presented in base64 or hex encoding.
* Support for TLS authentication.
* Basic cluster admin functions: Create & delete topics.

> [!NOTE]
> This repository is a fork of the original [kt](https://github.com/fgeller/kt).

## Examples

<details><summary>Read details about topics that match a regex</summary>

```sh
$ kt topic --filter news --partitions
{
  "name": "actor-news",
  "partitions": [
    {
      "id": 0,
      "oldest": 0,
      "newest": 0
    }
  ]
}
```
</details>

<details><summary>Produce messages</summary>

```sh
$ echo 'Alice wins Oscar' | kt produce --topic actor-news --literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ echo 'Bob wins Oscar' | kt produce --topic actor-news --literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ for i in {6..9} ; do echo Bourne sequel $i in production. | kt produce --topic actor-news --literal ;done
{
  "count": 1,
  "partition": 0,
  "startOffset": 1
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 2
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 3
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 4
}
```
</details>

<details><summary>Or pass in JSON object to control key, value and partition</summary>

```sh
$ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | kt produce --topic actor-news
{
  "count": 1,
  "partition": 0,
  "startOffset": 5
}
```
</details>

<details><summary>Advanced produce options</summary>

```sh
# With compression (gzip, snappy, lz4)
$ echo 'Compressed message' | kt produce --topic actor-news --literal --compression gzip

# With custom partitioner
$ echo '{"value": "message", "key": "test"}' | kt produce --topic actor-news --partitioner hashCode

# With batch processing and buffer size
$ cat messages.json | kt produce --topic actor-news --batch 100 --buffer-size 16384

# Decode input from base64 or hex
$ echo 'aGVsbG8gd29ybGQ=' | kt produce --topic actor-news --decode-value base64 --literal
```
</details>

<details><summary>Read messages at specific offsets on specific partitions</summary>

```sh
$ kt consume --topic actor-news --offsets 0=1:2
{
  "partition": 0,
  "offset": 1,
  "key": "",
  "value": "Bourne sequel 6 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 2,
  "key": "",
  "value": "Bourne sequel 7 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
```
</details>

<details><summary>Follow a topic, starting relative to newest offset</summary>

```sh
$ kt consume --topic actor-news --offsets all=newest-1:
{
  "partition": 0,
  "offset": 4,
  "key": "",
  "value": "Bourne sequel 9 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 5,
  "key": "Arni",
  "value": "Terminator terminated",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
^Creceived interrupt - shutting down
shutting down partition consumer for partition 0
```
</details>

<details><summary>Consume messages from specific timestamp</summary>

```sh
# Start from current time (equivalent to newest)
$ kt consume --topic actor-news --offsets now

# Start from specific absolute time (RFC3339 format)
$ kt consume --topic actor-news --offsets "2023-12-01T15:00:00Z"

# Start from 1 hour ago
$ kt consume --topic actor-news --offsets "-1h"

# Start from 30 minutes in the future
$ kt consume --topic actor-news --offsets "+30m"
```
</details>

<details><summary>Mix partition-specific and timestamp-based offsets</summary>

```sh
# Partition 0 from oldest, others from 1 hour ago
$ kt consume --topic actor-news --offsets "0=oldest,-1h"

# Specific partitions with absolute timestamp
$ kt consume --topic actor-news --offsets "1=2023-12-01T15:00:00Z,2=now"
```
</details>

<details><summary>View offsets for a given consumer group</summary>

```sh
$ kt group --group enews --topic actor-news --partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 6,
      "lag": 0
    }
  ]
}
```
</details>

<details><summary>Change consumer group offset</summary>

```sh
$ kt group --group enews --topic actor-news --partitions 0 --reset 1
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
$ kt group --group enews --topic actor-news --partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
```
</details>

<details><summary>Advanced group operations</summary>

```sh
# Filter groups by name pattern
$ kt group --filter-groups "^test-.*"

# Filter topics by name pattern with group info
$ kt group --group my-group --filter-topics "^actor-.*"

# Reset consumer group offset to specific timestamp
$ kt group --group my-group --topic actor-news --reset "2023-12-01T15:00:00Z"

# Reset to specific offset number
$ kt group --group my-group --topic actor-news --partitions 0,1 --reset 100

# Show offsets without fetching additional data
$ kt group --group my-group --offsets
```
</details>

<details><summary>Create and delete a topic</summary>

```sh
$ kt admin --create-topic morenews --topic-detail <(jsonify =NumPartitions 1 =ReplicationFactor 1)
$ kt topic --filter news
{
  "name": "morenews"
}
$ kt admin --delete-topic morenews
$ kt topic --filter news
```

</details>

<details><summary>Change broker address via environment variable</summary>

```sh
$ export KT_BROKERS=brokers.kafka:9092
$ kt <command> <option>
```

</details>

<details><summary>Using jq filters and raw output</summary>

```sh
# Apply jq filter to extract specific fields
$ kt consume --topic actor-news --offsets 0=0:1 --jq '.value'
"Bourne sequel 6 in production."

# Use raw output (like jq -r) to get unquoted strings
$ kt consume --topic actor-news --offsets 0=0:1 --jq '.value' --raw
Bourne sequel 6 in production.

# Extract and parse JSON values
$ kt consume --topic json-data --jq '.value | fromjson | .field'

# Filter messages by key
$ kt consume --topic actor-news --jq 'select(.key == "Arni")'

# Control pretty printing (enabled by default for terminals)
$ kt consume --topic actor-news --offsets 0=0:1 --no-pretty
{"partition":0,"offset":0,"key":"","value":"Alice wins Oscar","timestamp":"1970-01-01T00:59:59.999+01:00"}

# Use --compact or -c for compact output (same as --no-pretty)
$ kt consume --topic actor-news --offsets 0=0:1 --compact
{"partition":0,"offset":0,"key":"","value":"Alice wins Oscar","timestamp":"1970-01-01T00:59:59.999+01:00"}
```

</details>

## Installation

### Release binaries

You can download kt via the [Releases](https://github.com/fujiwara/kt/releases) section.

### Homebrew

```sh
$ brew install fujiwara/tap/kt
```

### Go install

```console
$ go install github.com/fujiwara/kt/v14@latest
```

## Usage:

    $ kt --help
    Usage: kt <command> [flags]

    Flags:
      -h, --help       Show context-sensitive help.
      -v, --version    Show version and exit.

    Commands:
      consume [flags]
        consume messages.

      produce [flags]
        produce messages.

      topic [flags]
        topic information.

      group [flags]
        consumer group information and modification.

      admin [flags]
        basic cluster administration.

    Run "kt <command> --help" for more information on a command.

## Authentication / Encryption

Authentication configuration is possibly via a JSON file. You indicate the mode
of authentication you need and provide additional information as required for
your mode. You pass the path to your configuration file via the `--auth` flag to
each command individually, or set it via the environment variable `KT_AUTH`.

### TLS

Required fields:

 - `mode`: This needs to be set to `TLS`
 - `client-certificate`: Path to your certificate
 - `client-certificate-key`: Path to your certificate key
 - `ca-certificate`: Path to your CA certificate

Example for an authorization configuration that is used for the system tests:


    {
        "mode": "TLS",
        "client-certificate": "test-secrets/kt-test.crt",
        "client-certificate-key": "test-secrets/kt-test.key",
        "ca-certificate": "test-secrets/snakeoil-ca-1.crt"
    }

If any certificate or key path is simply the name of the file, it is assumed to
be in the same directory as the auth file itself. For example if the path to the
auth file is `/some/dir/kt-auth.json` then a `"client-certificate":
"kt-test.crt"` will be qualified to `/some/dir/kt-test.crt`.

### TLS one-way

Required fields:

 - `mode`: This needs to be set to `TLS-1way`

Optional fields:

 - `ca-certificate`: Path to your CA certificate


Example:


    {
        "mode": "TLS-1way"
    }

### SASL

Required fields:

 - `mode`: This needs to be set to `SASL`
 - `sasl_user`: SASL username
 - `sasl_password`: SASL password

Optional fields:

 - `sasl_mechanism`: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512). Defaults to PLAIN

Example for PLAIN:

    {
        "mode": "SASL",
        "sasl_user": "your_username",
        "sasl_password": "your_password"
    }

Example for SCRAM-SHA-256:

    {
        "mode": "SASL",
        "sasl_user": "your_username",
        "sasl_password": "your_password",
        "sasl_mechanism": "SCRAM-SHA-256"
    }

### SASL_SSL / TLS-1way-SASL

This mode combines TLS encryption with SASL authentication.

Required fields:

 - `mode`: This needs to be set to `SASL_SSL` or `TLS-1way-SASL`
 - `sasl_user`: SASL username
 - `sasl_password`: SASL password

Optional fields:

 - `ca-certificate`: Path to your CA certificate
 - `sasl_mechanism`: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512). Defaults to PLAIN

Example with PLAIN:

    {
        "mode": "SASL_SSL",
        "sasl_user": "your_username",
        "sasl_password": "your_password",
        "ca-certificate": "ca-cert.pem"
    }

Example with SCRAM-SHA-512:

    {
        "mode": "SASL_SSL",
        "sasl_user": "your_username",
        "sasl_password": "your_password",
        "sasl_mechanism": "SCRAM-SHA-512",
        "ca-certificate": "ca-cert.pem"
    }

### Other modes

Please create an
[issue](https://github.com/fujiwara/kt/issues/new) with details for the mode that you need.
