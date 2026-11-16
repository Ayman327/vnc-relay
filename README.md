# vnc-relay

Public relay for a noVNC (VNC-over-WebSocket) desktop. Lets a phone browser
reach a VNC server running on a computer with no public IP.

## How it works

```
phone browser ──HTTPS/WSS──> this relay ──WSS──> VM client (vncrelay) ──> localhost:6080 (noVNC/websockify) ──> VNC :5900
```

The VM client keeps one WebSocket open to this relay (`/vm`). The relay
forwards the phone's HTTP requests (the noVNC page and assets) and its
WebSocket (`/websockify`, the VNC data channel) through that one connection.

## Deploy (Render, free)

1. Create a free account at https://render.com (sign in with GitHub).
2. New → Web Service → connect this repo.
3. Render reads `render.yaml` (Go, free plan) and deploys.
4. Note the public URL, e.g. `https://vnc-relay-xxxx.onrender.com`.

## Run the VM client

On the computer running the VNC server (x11vnc on :5900, websockify on :6080):

```
./vncrelay -worker https://vnc-relay-xxxx.onrender.com -local http://localhost:6080
```

Then open `https://vnc-relay-xxxx.onrender.com/vnc.html` on the phone and
enter the VNC password.

## Files

- `server.go` — the public relay (this repo's main package).
- `client/relay.go` — the VM-side client (build separately; see its own
  `go.mod` in `client/`).
- `worker.js` — an earlier Cloudflare Worker + Durable Object version of
  the same relay (kept for reference; not used by the Render deploy).
