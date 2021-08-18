# Publish & Subscribe Service for AssemblageDB

Assemblage Broadcast is a simple REST service that stores AssemblageDB nodes
published as broadcasts. It is implemented as a Cloudflare Worker and persists
all broadcasts using Workers KV. Broadcasting requires no authentication, but
all broadcasts expire automatically after 24 hours.

## Broadcast Format

Broadcasts are a mostly-append-only stream of bytes exported and published from
an AssemblageDB, which can then be subscribed to and imported in a different
remote AssemblageDB. Each broadcast is identified via a UUID that is randomly
generated together with an authorization UUID token by the Assemblage Broadcast
service when a broadcast is first uploaded. The original uploader can use the
authorization token to append updates (called "episodes") to the broadcast,
while receivers of the broadcast (with knowledge only of its public UUID, but
not the authorization token) can fetch the broadcast and all of its episodes,
but not modify it.

## Routes

### **POST** `/broadcast` & `/broadcast?episode={episode_id}`

Creates a new broadcast, identified by a new randomly generated UUID. If a body
(of bytes) is provided and the optional query parameter `?episode={episode_id}`
is set, an episode with the specified episode id and the body of bytes as the
content will be added to the broadcast. (This can be used to create a broadcast
and upload its content in a single call, it is equivalent to a `POST` without
episode id followed immediately by a `PUT` of the episode with the specified
id.)

**Response** (`201 CREATED`):

The `"expiration"` value returned as part of the response is the time of
expiration, measured in seconds since the Unix epoch.

```json
{
    "broadcast_id": "<randomly generated UUID, used to GET the broadcast>",
    "token": "<randomly generated UUID, used to POST/PUT the broadcast>",
    "expiration": 123456789
}
```

### **PUT** `/broadcast/{broadcast_id}/{episode_id}`

Uploads the body of bytes as the specified episode, associating it with the
specified broadcast. Requires an `Authorization: Bearer <token>`, otherwise a
`401 UNAUTHORIZED` will be returned.

**Response** (`201 CREATED` or `200 OK`):

```json
""
```

### **DELETE** `/broadcast/{broadcast_id}`

Deletes the specified broadcast by clearing its list of episodes. The episodes
will not be deleted and remain accessible to anyone with the episode ids. This
ensures that the broadcast can be overwritten with new episodes (as long as they
have new episode ids) but that clients that are in the process of downloading
old episodes can continue to do so until the episodes expire automatically
(after 24 hours).

**Response** (`200 OK`):

```json
""
```

### **GET** `/broadcast/{broadcast_id}`

Returns a list of all episodes associated with the broadcast.

**Response** (`200 OK`):

```json
[
    "<episode_id1>",
    "<episode_id2>"
]
```

### **GET** `/broadcast/{broadcast_id}/{episode_id}`

Returns the bytes of the specified episode in the specified broadcast.

**Response** (`200 OK`):

```text
<the bytes of the episode>
```
