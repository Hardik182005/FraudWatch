# FraudWatch capture evidence

Everything in this folder was captured on **2026-09-18** from the **deployed Cloud Run sites**, in a
real headless Chromium (Playwright 1.63.0) — not from a local dev server and not hand-edited.

| Folder | Contents |
|---|---|
| `screenshots/` | 14 PNGs — every route of both sites, plus a Pixel 7 mobile shot of each landing page — and `capture-log.json` |
| `video/` | `live-walkthrough.webm`, `lab-walkthrough.webm`, and `tour-log.json` (per-route HTTP status + every console error) |
| `qr/` | QR codes for the two deployed site URLs |

Regenerate with `node scripts/capture_screenshots.mjs` and `node scripts/capture_video.mjs`.
Both scripts exit non-zero if any route is not HTTP 2xx.

## What these show

The Live capture drives the app the way a visitor does: it starts a real demo session, starts a real
bounded `ieee_replay` run from `/scenario`, waits for the session's events-used counter to move, and
only then photographs the console. So the Live shots and recording carry **real scored data** — at
capture time the console read 274 scored, 80 alerts, 8 reviews, 4.6 ev/s, end-to-end p50 110 ms /
p95 167 ms / p99 186 ms, with `R1_BURST` firing in the alert queue.

All 12 routes returned HTTP 200, WebGL initialised on every 3D hero, and the capture recorded
**0 console errors** (8 remaining messages are THREE.js deprecation and WebGL performance warnings).

Two honest caveats:

- The console figures above are a **live UI reading at one instant while the run was still in flight**.
  They are a snapshot, not a total, and not a controlled benchmark — the measured runs are in
  `docs/measured-results.md`.
- An **earlier capture the same day showed every data panel empty**, because of a CORS defect that
  blocked all API calls from the deployed sites (192 console errors). That defect was fixed — Cloud Run
  revision `fraudwatch-control-00004-dgw` — and these files are from the recapture afterwards. The
  story is written up in `docs/measured-results.md` §7.1 and the report §8.
