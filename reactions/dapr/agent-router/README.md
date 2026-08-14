# Dapr Agent Router Reaction

## Limitations
- Only supports a single message broker type
- Only supports one replica
- Subscription ID is currently the client-provided agent ID (which can be anything). It needs to be composed with a principal (e.g. API key ID, OAuth sub, SPIFFE ID) in authenticated mode
or source (e.g. registered workflow ID) in unauthenticated mode?