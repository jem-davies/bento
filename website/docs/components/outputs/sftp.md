---
title: sftp
type: output
status: experimental
categories: ["Network"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/output/sftp.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

EXPERIMENTAL: This component is experimental and therefore subject to change or removal outside of major version releases.
Writes files to a server over SFTP.

Introduced in version 3.39.0.

```yaml
# Config fields, showing default values
output:
  sftp:
    address: ""
    path: ""
    codec: all-bytes
    credentials:
      username: ""
      password: ""
    max_in_flight: 1
```

In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

## Performance

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.

## Fields

### `address`

The address of the server to connect to that has the target files.


Type: `string`  
Default: `""`  

### `path`

The file to save the messages to on the server.


Type: `string`  
Default: `""`  

### `codec`

The way in which the bytes of messages should be written out into the output file. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.


Type: `string`  
Default: `"all-bytes"`  

| Option | Summary |
|---|---|
| `all-bytes` | Write the message to the file in full. If the file already exists the old content is deleted. |
| `lines` | Append messages to the file followed by a line break. |
| `delim:x` | Append messages to the file followed by a custom delimiter. |


```yaml
# Examples

codec: lines

codec: "delim:\t"

codec: delim:foobar
```

### `credentials`

The credentials to use to log into the server.


Type: `object`  

### `credentials.username`

The username to connect to the SFTP server.


Type: `string`  
Default: `""`  

### `credentials.password`

The password for the username to connect to the SFTP server.


Type: `string`  
Default: `""`  

### `max_in_flight`

The maximum number of messages to have in flight at a given time. Increase this to improve throughput.


Type: `number`  
Default: `1`  

