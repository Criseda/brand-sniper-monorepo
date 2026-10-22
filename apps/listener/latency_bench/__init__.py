"""
Ingress to decision latency benchmark for the Skinport listener (#175).

A local stand-in for Skinport's Socket.IO sale feed plays a fixed, seeded scenario into the real ingress path
(the Node.js sidecar, Redis Pub/Sub, `main.tick_consumer`, the price window, the Z score and the Edge DRE),
and every stage is timed on one monotonic clock. Nothing contacts Skinport or the backend.

Usage and method: docs/benchmarks/ingress_latency.md.
"""
