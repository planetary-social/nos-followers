## Followers server
A server that listens to Nostr [Follow List](https://github.com/nostr-protocol/nips/blob/master/02.md) events and identifies follow and unfollow actions. These events are then published to a Google Pub/Sub queue.

### Local development
The server, a nostr relay and a postgres db are created through `docker compose --build`.
Then you can `psql "postgres://postgres:mydevpassword@localhost:5432/followers"` to explore the DB.
The DB will be created if not present and any pending migration will be run each time the server starts
The local relay will be available at `ws://localhost:7777`

## Contributing
Contributions are welcome! Fork the project, submit pull requests, or report issues.

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

