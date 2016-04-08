# raven-changes-fail-over
A quick-and dirty demonstration of how to support fail-over for the RavenDB change-notification API.

You will need at least 3 RavenDB instances in order to be able to continue to receive notifications when an instance is stopped (probably because if you have only 2 servers, then the system does not have a quorum).
Create databases with the same name and replication bundle enabled on each of the servers, and configure them to replicate together.
Edit the primary and secondary server URLs, create a test document, and then start the program.

When you create or modify a document you should see a notification from the most-preferred server.
If you stop a server, you should see see notifications coming from the next-most-preferred server.

There's a bit of a race condition regarding subscribe / unsubscribe (you may receive the same notification from 2 servers or miss the notification alltogether) but the code can be tuned in either direction (at-most-once or at-least-once) but keep in mind that unlike data subscriptions, change notifications are best-effort only.

Note that you can also configure fail-over servers via the connection string.