### Fixed

- **A Kobo sync no longer fails when another writer holds `app.db` as the response is staged (#2562).** The sync's final write — the replayable copy of the response, which only makes the next sync a delta — is now best-effort: it retries briefly, and if the writer is still held the request rolls back and delivers the response anyway instead of turning a finished response into a 500. The device then does a full sync next time rather than receiving nothing.
