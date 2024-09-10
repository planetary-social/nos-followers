## Followers server
[![Coverage Status](https://coveralls.io/repos/github/planetary-social/nos-followers/badge.svg?branch=main)](https://coveralls.io/github/planetary-social/nos-followers?branch=main)

A server that listens to Nostr [Follow List](https://github.com/nostr-protocol/nips/blob/master/02.md) events and identifies follow and unfollow actions. These events are then published to a Google Pub/Sub queue.

### Local development
The server, a nostr relay and a postgres db are created through `docker compose up --build` which is a good way to test docker configurations.
Most often than not, you'll want to select only relay and db through `docker compose up relay db` and run the server in your host with a simple `cargo run`
The DB can be explored by visiting `http://localhost:7474/browser/`
The DB will be created if not present and any pending migration will be run each time the server starts
The local relay will be available at `ws://localhost:7777`, ensure that you have a `config/settings.development.yml` connected to the local relay:

```
relay: "ws://relay:7777"
```

Once the server is running, an easy way to test is using nak with kind 3 events that add different npubs to the list:
```
nak event -k 3 -t 'p=7286f8fc095cfa1de9b08afcf8adacdccf75e8c337a09407ec713c751202d894' -t 'p=7286f8fc095cfa1de9b08afcf8adacdccf75e8c337a09407ec713c751202d897' ws://localhost:7777
```

### Deployment

When changes are pushed to the `main` branch and tests pass, the `latest` Docker image is created, but this does not trigger a deployment.
To trigger a deployment, promote the `latest` image to `stable` by running `./scripts/tag_latest_as_stable.sh`.

## Contributing
Contributions are welcome! Fork the project, submit pull requests, or report issues.

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

