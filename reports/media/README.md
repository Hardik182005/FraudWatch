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

## Read these honestly

All 12 routes returned HTTP 200 and WebGL initialised on every 3D hero. **But the data panels appear
empty or in an error state**, because at capture time every API call from both deployed sites was
blocked by CORS (192 console errors, all the same failure — see `video/tour-log.json` and §8 of
`../FraudWatch-Report.md`). These files are honest evidence that both sites deploy, route and render.
They are **not** evidence of the pipeline serving live data. Recapture after the CORS fix.
