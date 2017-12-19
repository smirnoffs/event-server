### The Protocol
The protocol used by the clients is string-based (i.e. a `CRLF` control
character terminates each message). All strings are encoded in `UTF-8`.

The *event source* **connects on port 9090** and will start sending
events as soon as the connection is accepted.

The many *user clients* will **connect on port 9099**. As soon
as the connection is accepted, they will send to the server the ID of
the represented user, so that the server knows which events to
inform them of. For example, once connected a *user client* may send down:
`2932\n`, indicating that they are representing user 2932.

After the identification is sent, the *user client* starts waiting for
events to be sent to them. Events coming from *event source* should be
sent to relevant *user clients* exactly like read, no modification is
required or allowed.

### The Events
There are five possible events. The table below describe payloads
sent by the *event source* and what they represent:

| Payload       | Sequence #| Type         | From User Id | To User Id |
|---------------|-----------|--------------|--------------|------------|
|666\|F\|60\|50 | 666       | Follow       | 60           | 50         |
|1\|U\|12\|9    | 1         | Unfollow     | 12           | 9          |
|542532\|B      | 542532    | Broadcast    | -            | -          |
|43\|P\|32\|56  | 43        | Private Msg  | 32           | 56         |
|634\|S\|32     | 634       | Status Update| 32           | -          |

* **Follow**: Only the `To User Id` should be notified
* **Unfollow**: No clients should be notified
* **Broadcast**: All connected *user clients* should be notified
* **Private Message**: Only the `To User Id` should be notified
* **Status Update**: All current followers of the `From User ID` should be notified