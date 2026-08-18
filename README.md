# sump-pump-consumer
Consumes sump pump events from NATS JetStream and processes alerts

## Deployment

CI builds an ARM64 image, pushes it to GHCR, then commits the new tag to the `sump-pump` workspace in [homelab-workspaces](https://github.com/cujarrett/homelab-workspaces). ArgoCD deploys from there.

### Rotating `HOMELAB_PAT`

Shared across all `homelab-workspaces`-deploying repos and rotated centrally - see
[GitHub Tokens](https://github.com/cujarrett/homelab/blob/main/docs/github-tokens.md) in the
homelab repo. When it expires, `deploy` fails on `Bad credentials (HTTP 401)` while `test` and
`build-and-push` stay green - images keep building, the cluster keeps running the old tag.
