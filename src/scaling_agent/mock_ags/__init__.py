"""A local stand-in for Tencent Cloud AGS, for developing and testing without the cloud.

It speaks the real protocols, so the production code paths run unchanged:

* control plane: Tencent Cloud API v3 (`POST /` + `X-TC-Action`, TC3-HMAC-SHA256 signatures
  verified against a configured SecretId/SecretKey), the AGS actions the launcher uses;
* data plane: a gateway in front of each sandbox's envd (E2B protocol), routed by the
  `E2b-Sandbox-Id` header like E2B's client proxy, authenticated with `X-Access-Token`;
* sandboxes: Kubernetes pods whose main process is a real envd (built from e2b-dev/infra),
  or a static address for tests.

Behaviours the AGS docs leave open are modelled on the evidence we have and flagged in
`docs/AGS.md`; `scripts/ags_probe.py` checks them against the real service.
"""
