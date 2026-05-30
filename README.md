## Followers server
[![codecov](https://codecov.io/gh/planetary-social/nos-followers/graph/badge.svg?token=3B19KU1247)](https://codecov.io/gh/planetary-social/nos-followers)

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

## OpenRanking API

This server implements the subset of the [Open Ranking protocol](https://github.com/Open-Ranking/protocol) that maps onto our follow graph: PageRank-based ranking and reputation, top followers, and personalised recommendations. These endpoints sit alongside the existing `/api/v1` API and speak JSON over HTTP. Pubkeys are 64-char lowercase hex; errors carry a human-readable `X-Reason` header.

Discover the supported endpoints and algorithms from the capability document:

```
GET /.well-known/open-ranking
```

```json
{
  "/rank":       [{ "id": "pagerank", "description": "Global PageRank over the Nostr follow graph. Higher is better." }],
  "/reputation": [{ "id": "pagerank", "description": "PageRank reputation with follower and following counts." }],
  "/followers":  [{ "id": "pagerank", "description": "Top followers ranked by global PageRank." }],
  "/recommend":  [{ "id": "for-you", "pov": true, "description": "Personalised follow recommendations from your social graph." }]
}
```

Endpoints (the default algorithm is the first listed for each):

- `POST /rank` — rank a set of pubkeys by global PageRank. Body: `{ "pubkeys": [..], "limit"?: n }`. Returns a `rank` for every requested pubkey (unknown pubkeys rank `0`), sorted descending: `{ "profiles": [{ "pubkey", "rank" }], "ttl" }`.
- `POST /reputation` — structured reputation for one pubkey. Body: `{ "pubkey": ".." }`. Returns `{ "pubkey", "rank" (PageRank), "stats": { "followers", "follows" }, "x": { "trusted" }, "ttl" }`.
- `POST /followers` — a pubkey's top followers by PageRank. Body: `{ "pubkey": ".." , "limit"?: n }`. Returns `{ "profiles", "total", "ttl" }`.
- `POST /recommend` — personalised recommendations (`for-you` requires a `pov`). Body: `{ "pov": ".." , "limit"?: n }` (`limit` defaults to 10 and may not exceed 10; larger values are rejected). Returns `{ "profiles", "ttl" }` ranked by social-graph similarity. The heavy similarity query is computed in the background (shared with the `/api/v1/recommendations` queue) so the endpoint never blocks: a cold account returns an empty `profiles` with a short `ttl`; retry after the `ttl` once the result is warmed.

PageRank is recomputed on a daily cron, which the `ttl` hint reflects. `/search/profiles` (ORE-05) is not implemented as we do not maintain a profile text index.

## Contributing
Contributions are welcome! Fork the project, submit pull requests, or report issues.

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

