### Local development
The server, a nostr relay and a postgres db are created through `docker compose --build`.
Then you can `psql "postgres://postgres:mydevpassword@localhost:5432/followers"` to explore the DB.
The DB will be created if not present and any pending migration will be run each time the server starts
The relay will be available at `ws://localhost:7777`
