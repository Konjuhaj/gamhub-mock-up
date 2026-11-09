inital commit

## Privacy

Minors and deleted accounts never leave our infrastructure. The consumer looks up every event's player in `player_state` before any Braze payload is built and drops the event if the player is deleted, a minor, has an unknown age, or is not found (fail closed). Dropped events are counted in `suppressed_total{reason="deleted|minor|age_unknown"}`, are still checkpointed so they are not replayed, and are only logged as a hashed `player_id` plus the reason. `TestReplayOnlyAdultReachesBraze` proves that only the adult player reaches Braze.

These are hard rules, so they are enforced in-stream: data minimisation means data we are not allowed to use should not be sent at all. Channel consent (email, push opt-in) is different. It changes often and belongs to the user, so it stays in Braze subscription groups and is checked at send time, not built here.

Next steps (out of scope for the POC): erasure across a warehouse, retention jobs and subscription-group sync.
