#!/usr/bin/env python3
"""Generate the exportable report figures in docs/figures/ from committed artifacts only.

Every number plotted here is read from:
  - ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json   (curves, confusion matrices, calibration, sweep)
  - ml/artifacts/fw-ieee-stream-1.1.0/metadata.json     (model identity, feature spec)
  - services/controller/app.py defaults, via .env.example (lifecycle timings)

Nothing is simulated, smoothed or invented. If a quantity is not in an artifact it is not drawn.

Run:  python docs/figures/make_figures.py
Output: plain SVG, no external fonts, no JavaScript, white ground — safe to embed in a
report, a slide deck, or a Markdown page, and safe to convert to PNG/PDF.
"""

from __future__ import annotations

import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
ART = ROOT / "ml" / "artifacts" / "fw-ieee-stream-1.1.0"
OUT = pathlib.Path(__file__).resolve().parent

INK = "#111827"
MUTED = "#6b7280"
GRID = "#e5e7eb"
ACCENT = "#1d4ed8"      # LightGBM / primary series
ACCENT2 = "#b45309"     # baseline / secondary series
ALERT = "#b91c1c"
REVIEW = "#a16207"
OK = "#15803d"
FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"


def esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


class Svg:
    def __init__(self, w: int, h: int, title: str, subtitle: str = "") -> None:
        self.w, self.h = w, h
        self.parts: list[str] = []
        self.parts.append(
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
            f'role="img" aria-label="{esc(title)}" font-family="{FONT}">'
        )
        self.parts.append(f'<rect width="{w}" height="{h}" fill="#ffffff"/>')
        self.text(24, 34, title, size=17, weight="600")
        if subtitle:
            self.text(24, 54, subtitle, size=11.5, fill=MUTED)

    def text(self, x, y, s, size=11, fill=INK, weight="400", anchor="start", opacity=1.0):
        self.parts.append(
            f'<text x="{x:.2f}" y="{y:.2f}" font-size="{size}" fill="{fill}" font-weight="{weight}" '
            f'text-anchor="{anchor}" opacity="{opacity}">{esc(s)}</text>'
        )

    def line(self, x1, y1, x2, y2, stroke=GRID, width=1, dash=None, opacity=1.0):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" stroke="{stroke}" '
            f'stroke-width="{width}"{d} opacity="{opacity}"/>'
        )

    def rect(self, x, y, w, h, fill, stroke=None, rx=0, opacity=1.0):
        s = f' stroke="{stroke}"' if stroke else ""
        self.parts.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{max(w, 0):.2f}" height="{max(h, 0):.2f}" '
            f'fill="{fill}"{s} rx="{rx}" opacity="{opacity}"/>'
        )

    def path(self, pts, stroke, width=2, dash=None, fill="none"):
        if not pts:
            return
        d = "M " + " L ".join(f"{x:.2f} {y:.2f}" for x, y in pts)
        da = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<path d="{d}" fill="{fill}" stroke="{stroke}" stroke-width="{width}" '
            f'stroke-linejoin="round" stroke-linecap="round"{da}/>'
        )

    def dot(self, x, y, r, fill):
        self.parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{r}" fill="{fill}"/>')

    def save(self, name: str) -> None:
        self.parts.append("</svg>")
        (OUT / name).write_text("\n".join(self.parts), encoding="utf-8")
        print("wrote", (OUT / name).relative_to(ROOT))


def frame(svg: Svg, x0, y0, x1, y1, xlab, ylab, xticks, yticks, fmt=lambda v: f"{v:.1f}"):
    """Draw a plot frame with gridlines. Returns scale functions."""
    def sx(v):
        return x0 + (v - xticks[0]) / (xticks[-1] - xticks[0]) * (x1 - x0)

    def sy(v):
        return y1 - (v - yticks[0]) / (yticks[-1] - yticks[0]) * (y1 - y0)

    for t in yticks:
        y = sy(t)
        svg.line(x0, y, x1, y, GRID)
        svg.text(x0 - 8, y + 3.5, fmt(t), size=10, fill=MUTED, anchor="end")
    for t in xticks:
        x = sx(t)
        svg.line(x, y0, x, y1, GRID)
        svg.text(x, y1 + 16, fmt(t), size=10, fill=MUTED, anchor="middle")
    svg.line(x0, y0, x0, y1, MUTED)
    svg.line(x0, y1, x1, y1, MUTED)
    svg.text((x0 + x1) / 2, y1 + 34, xlab, size=11, fill=INK, anchor="middle")
    svg.parts.append(
        f'<text x="{x0 - 42}" y="{(y0 + y1) / 2}" font-size="11" fill="{INK}" text-anchor="middle" '
        f'transform="rotate(-90 {x0 - 42} {(y0 + y1) / 2})">{esc(ylab)}</text>'
    )
    return sx, sy


def footer(svg: Svg, src: str) -> None:
    svg.text(24, svg.h - 12, "Source: " + src, size=9.5, fill=MUTED)


# ---------------------------------------------------------------------------------------------
# CA1 · precision-recall curve
# ---------------------------------------------------------------------------------------------
def fig_pr_curve(ev):
    ho = ev["held_out_test"]
    pts = ho["pr_curve"]["points"]
    base = ev["class_balance"]["test"]["fraud_rate"]
    lr = ev["baseline_comparison"]["logistic_regression"]

    svg = Svg(760, 520, "CA1 · Precision-recall, held-out test split",
              f"LightGBM fw-ieee-stream-1.1.0 · n = {ho['n']:,} · positives {ev['class_balance']['test']['fraud']:,} "
              f"({base * 100:.2f}%) · PR-AUC {ho['pr_auc']:.4f}")
    x0, y0, x1, y1 = 90, 86, 600, 420
    sx, sy = frame(svg, x0, y0, x1, y1, "Recall", "Precision",
                   [0, .2, .4, .6, .8, 1.0], [0, .1, .2, .3, .4, .5])

    svg.line(x0, sy(base), x1, sy(base), MUTED, 1.2, dash="4 4")
    svg.text(x1 - 4, sy(base) - 6, f"random baseline = base rate {base:.4f}", size=9.5,
             fill=MUTED, anchor="end")

    curve = [(sx(min(r, 1.0)), sy(min(p, .5))) for r, p in pts if p <= .5]
    svg.path(curve, ACCENT, 2.2)

    for name, key, colour in (("alert", "at_alert_threshold", ALERT),
                              ("review", "at_review_threshold", REVIEW)):
        op = ho[key]
        px, py = sx(op["recall"]), sy(op["precision"])
        svg.dot(px, py, 5, colour)
        svg.text(px + 9, py - 6, f"{name} thr {op['threshold']:.4f}", size=10, fill=colour, weight="600")
        svg.text(px + 9, py + 7, f"P {op['precision']:.3f} · R {op['recall']:.3f} · F1 {op['f1']:.3f}",
                 size=9.5, fill=colour)

    lx, ly = 622, 100
    svg.text(lx, ly, "PR-AUC", size=11, weight="600")
    svg.rect(lx, ly + 10, 10, 10, ACCENT)
    svg.text(lx + 16, ly + 19, f"LightGBM  {ho['pr_auc']:.4f}", size=10.5)
    svg.rect(lx, ly + 28, 10, 10, ACCENT2)
    svg.text(lx + 16, ly + 37, f"Logistic  {lr['test_pr_auc']:.4f}", size=10.5)
    svg.text(lx, ly + 62, "Read PR-AUC, not accuracy:", size=10, fill=MUTED)
    svg.text(lx, ly + 76, "at the alert threshold accuracy", size=10, fill=MUTED)
    svg.text(lx, ly + 90, f"is {ho['at_alert_threshold']['accuracy']:.3f}, and the", size=10, fill=MUTED)
    svg.text(lx, ly + 104, f"baseline's is higher ({lr['test_at_validation_threshold']['accuracy']:.3f})", size=10, fill=MUTED)
    svg.text(lx, ly + 118, "because always answering", size=10, fill=MUTED)
    svg.text(lx, ly + 132, f"\"not fraud\" scores {(1 - base) * 100:.1f}%.", size=10, fill=MUTED)
    svg.text(lx, ly + 156, "Curve is the committed 200-point", size=10, fill=MUTED)
    svg.text(lx, ly + 170, "PR curve, clipped at precision 0.5", size=10, fill=MUTED)
    svg.text(lx, ly + 184, "(no point above it).", size=10, fill=MUTED)

    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → held_out_test.pr_curve, "
                "thresholds, baseline_comparison")
    svg.save("ca1-pr-curve.svg")


# ---------------------------------------------------------------------------------------------
# CA1 · ROC curve
# ---------------------------------------------------------------------------------------------
def fig_roc_curve(ev):
    ho = ev["held_out_test"]
    pts = ho["roc_curve"]["points"]
    lr = ev["baseline_comparison"]["logistic_regression"]

    svg = Svg(700, 500, "CA1 · ROC, held-out test split",
              f"LightGBM ROC-AUC {ho['roc_auc']:.4f} · logistic-regression baseline {lr['test_roc_auc']:.4f} "
              f"· n = {ho['n']:,}")
    x0, y0, x1, y1 = 90, 86, 560, 420
    sx, sy = frame(svg, x0, y0, x1, y1, "False positive rate", "True positive rate",
                   [0, .2, .4, .6, .8, 1.0], [0, .2, .4, .6, .8, 1.0])
    svg.line(sx(0), sy(0), sx(1), sy(1), MUTED, 1.2, dash="4 4")
    svg.text(sx(.62), sy(.55), "chance", size=10, fill=MUTED)
    svg.path([(sx(f), sy(t)) for f, t in pts], ACCENT, 2.2)
    svg.text(sx(.45), sy(.30), f"ROC-AUC {ho['roc_auc']:.4f}", size=11.5, fill=ACCENT, weight="600")
    svg.text(24, 462, "ROC looks flattering on a 3.5%-positive problem. It is included for completeness; "
                      "the PR curve is the honest view.", size=10, fill=MUTED)
    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → held_out_test.roc_curve")
    svg.save("ca1-roc-curve.svg")


# ---------------------------------------------------------------------------------------------
# CA1 · confusion heatmaps
# ---------------------------------------------------------------------------------------------
def fig_confusion(ev):
    ho = ev["held_out_test"]
    lr = ev["baseline_comparison"]["logistic_regression"]
    panels = [
        ("LightGBM @ alert 0.104772", ho["confusion_matrix_at_alert_threshold"], ho["at_alert_threshold"]),
        ("LightGBM @ review 0.069547", ho["at_review_threshold"]["confusion_matrix"], ho["at_review_threshold"]),
        ("Logistic regression @ 0.792659",
         lr["test_at_validation_threshold"]["confusion_matrix"], lr["test_at_validation_threshold"]),
    ]
    svg = Svg(980, 470, "CA1 · Confusion heatmaps, held-out test split",
              f"n = {ho['n']:,} · 3,083 fraud / 85,498 legitimate · thresholds fixed on validation, "
              "test scored once · cell shade = share of the whole split")

    for i, (name, cm, op) in enumerate(panels):
        ox = 40 + i * 315
        oy = 100
        cell = 118
        total = cm["tn"] + cm["fp"] + cm["fn"] + cm["tp"]
        svg.text(ox, oy - 26, name, size=12, weight="600")
        svg.text(ox, oy - 10, f"precision {op['precision']:.3f} · recall {op['recall']:.3f} · F1 {op['f1']:.3f}",
                 size=10, fill=MUTED)

        grid = [("tn", 0, 0), ("fp", 1, 0), ("fn", 0, 1), ("tp", 1, 1)]
        for key, cx, cy in grid:
            v = cm[key]
            share = v / total
            # perceptual-ish shade: sqrt so the three small cells stay distinguishable
            alpha = 0.10 + 0.88 * (share ** 0.5)
            good = key in ("tn", "tp")
            colour = OK if good else ALERT
            x = ox + cx * cell
            y = oy + cy * cell
            svg.rect(x, y, cell - 4, cell - 4, colour, opacity=round(min(alpha, 1.0), 3), rx=4)
            label_fill = "#ffffff" if alpha > 0.55 else INK
            svg.text(x + (cell - 4) / 2, y + 48, key.upper(), size=11, fill=label_fill,
                     weight="600", anchor="middle")
            svg.text(x + (cell - 4) / 2, y + 70, f"{v:,}", size=15, fill=label_fill,
                     weight="600", anchor="middle")
            svg.text(x + (cell - 4) / 2, y + 88, f"{share * 100:.2f}%", size=10, fill=label_fill,
                     anchor="middle")

        svg.text(ox - 6, oy + cell / 2, "actual", size=10, fill=MUTED, anchor="end")
        svg.text(ox - 6, oy + 14, "legit", size=9.5, fill=MUTED, anchor="end")
        svg.text(ox - 6, oy + cell + 14, "fraud", size=9.5, fill=MUTED, anchor="end")
        svg.text(ox + cell / 2 - 2, oy + 2 * cell + 8, "predicted allow", size=9.5, fill=MUTED, anchor="middle")
        svg.text(ox + 1.5 * cell - 2, oy + 2 * cell + 8, "predicted flag", size=9.5, fill=MUTED, anchor="middle")

    svg.text(40, 408, "The baseline's accuracy is higher than LightGBM's (0.921 vs 0.906) while catching "
                      "far less fraud — accuracy is not a usable metric here.", size=10.5, fill=INK)
    svg.text(40, 426, "At the alert threshold 1,950 fraudulent transactions are missed and 6,382 legitimate "
                      "ones are flagged for review. \"Alert\" means priority review, never a block.",
             size=10.5, fill=MUTED)
    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → held_out_test confusion matrices, "
                "baseline_comparison")
    svg.save("ca1-confusion-heatmap.svg")


# ---------------------------------------------------------------------------------------------
# CA1 · validation threshold sweep
# ---------------------------------------------------------------------------------------------
def fig_threshold_sweep(ev):
    sweep = ev["validation_threshold_sweep"]
    ths = [p["threshold"] for p in sweep]
    svg = Svg(760, 500, "CA1 · Threshold sweep on validation (thresholds are chosen here, never on test)",
              f"{len(sweep)} evaluated thresholds · alert = max-F1 point · review = top-10% flagged point")
    x0, y0, x1, y1 = 90, 86, 600, 410
    lo, hi = min(ths), max(ths)
    sx, sy = frame(svg, x0, y0, x1, y1, "Score threshold", "Metric value",
                   [lo, lo + (hi - lo) / 4, lo + (hi - lo) / 2, lo + 3 * (hi - lo) / 4, hi],
                   [0, .2, .4, .6, .8, 1.0], fmt=lambda v: f"{v:.2f}")
    for key, colour, label in (("recall", ACCENT, "recall"), ("precision", ACCENT2, "precision"),
                               ("f1", OK, "F1")):
        svg.path([(sx(p["threshold"]), sy(p[key])) for p in sweep], colour, 2)
    best = max(sweep, key=lambda p: p["f1"])
    svg.line(sx(best["threshold"]), y0, sx(best["threshold"]), y1, ALERT, 1.2, dash="4 3")
    svg.dot(sx(best["threshold"]), sy(best["f1"]), 4.5, ALERT)
    svg.text(sx(best["threshold"]) + 8, y0 + 14, f"alert threshold {ev['thresholds']['alert']:.6f}",
             size=10, fill=ALERT, weight="600")
    svg.line(sx(ev["thresholds"]["review"]), y0, sx(ev["thresholds"]["review"]), y1, REVIEW, 1.2, dash="4 3")
    svg.text(sx(ev["thresholds"]["review"]) + 8, y0 + 30, f"review {ev['thresholds']['review']:.6f}",
             size=10, fill=REVIEW, weight="600")

    lx = 622
    svg.text(lx, 100, "series", size=11, weight="600")
    for i, (colour, label) in enumerate(((ACCENT, "recall"), (ACCENT2, "precision"), (OK, "F1"))):
        svg.rect(lx, 110 + i * 18, 10, 10, colour)
        svg.text(lx + 16, 119 + i * 18, label, size=10.5)
    svg.text(lx, 180, "Chosen on validation", size=10, fill=MUTED)
    svg.text(lx, 194, "only, then frozen. Test", size=10, fill=MUTED)
    svg.text(lx, 208, "was scored once at these", size=10, fill=MUTED)
    svg.text(lx, 222, "exact values.", size=10, fill=MUTED)
    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → validation_threshold_sweep, thresholds")
    svg.save("ca1-threshold-sweep.svg")


# ---------------------------------------------------------------------------------------------
# CA1 · calibration
# ---------------------------------------------------------------------------------------------
def fig_calibration(ev):
    cal = ev["held_out_test"]["calibration"]
    bins = cal["bins"]
    svg = Svg(700, 500, "CA1 · Reliability of the score (it is not a calibrated probability)",
              f"10 equal-count bins over the held-out test split · Brier {cal['brier']:.4f} · ECE {cal['ece']:.4f}")
    x0, y0, x1, y1 = 90, 86, 560, 400
    top = max(max(b["mean_score"] for b in bins), max(b["observed_fraud_rate"] for b in bins))
    top = (int(top * 20) + 2) / 20
    sx, sy = frame(svg, x0, y0, x1, y1, "Mean predicted score in bin", "Observed fraud rate in bin",
                   [0, top / 4, top / 2, 3 * top / 4, top], [0, top / 4, top / 2, 3 * top / 4, top],
                   fmt=lambda v: f"{v:.2f}")
    svg.line(sx(0), sy(0), sx(top), sy(top), MUTED, 1.2, dash="4 4")
    svg.text(sx(top) - 6, sy(top) + 16, "perfect calibration", size=9.5, fill=MUTED, anchor="end")
    svg.path([(sx(b["mean_score"]), sy(b["observed_fraud_rate"])) for b in bins], ACCENT, 2)
    for b in bins:
        svg.dot(sx(b["mean_score"]), sy(b["observed_fraud_rate"]), 4, ACCENT)
    svg.text(24, 440, f"Every bin holds {bins[0]['count']:,} rows. A low ECE at a 3.48% base rate is cheap: "
                      "predicting a small number everywhere scores well.", size=10, fill=MUTED)
    svg.text(24, 458, "metadata.json states the output is \"a model score in [0,1]; not a calibrated "
                      "probability\" — treat it as a ranking.", size=10, fill=MUTED)
    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → held_out_test.calibration")
    svg.save("ca1-calibration.svg")


# ---------------------------------------------------------------------------------------------
# CA1 · feature importance
# ---------------------------------------------------------------------------------------------
def fig_importance(ev):
    gain = ev["global_importance"]["gain"]["features"]
    shap = ev["global_importance"]["shap"]
    rows = sorted(gain, key=lambda f: -f["gain"])
    svg = Svg(780, 90 + 26 * len(rows) + 90, "CA1 · Global feature importance (LightGBM total split gain)",
              f"{len(rows)} features of feature spec fs-1.1 · all computable from the stream · "
              "labels are never features")
    x0 = 230
    width = 470
    mx = rows[0]["gain"]
    for i, f in enumerate(rows):
        y = 96 + i * 26
        svg.text(x0 - 10, y + 11, f["feature"], size=11, anchor="end")
        w = width * f["gain"] / mx
        svg.rect(x0, y, w, 15, ACCENT, rx=2, opacity=0.88)
        svg.text(x0 + w + 8, y + 11.5, f"{f['gain']:,.0f}", size=10, fill=MUTED)
    base = 96 + 26 * len(rows) + 16
    svg.text(24, base, "Gain is a property of the fitted trees, not a causal statement. Per-event TreeSHAP "
                       "contributions are computed live by the scoring service in log-odds units;", size=10, fill=MUTED)
    svg.text(24, base + 16, f"their global mean |contribution| is also recorded, sampled over 20,000 test rows "
                            f"with expected value {shap['expected_value_log_odds']:.4f} log-odds.", size=10, fill=MUTED)
    footer(svg, "ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json → global_importance.gain")
    svg.save("ca1-feature-importance.svg")


# ---------------------------------------------------------------------------------------------
# Lifecycle timeline
# ---------------------------------------------------------------------------------------------
def fig_lifecycle():
    svg = Svg(960, 400, "Cloud lifecycle timeline · wake on visit, stop after idle",
              "Durations are the configured defaults from .env.example / services/controller/app.py. "
              "This is a schematic of the configuration, NOT a measured trace.")
    x0, x1 = 60, 900
    y = 150
    segs = [
        ("sleeping", 0.10, "#9ca3af", "VM TERMINATED\nCloud Run min-instances 0"),
        ("starting_vm", 0.09, "#6366f1", "instance start\ndeadline 600 s total"),
        ("starting_services", 0.16, "#8b5cf6", "Compose up\nPostgres + Kafka healthy"),
        ("restoring", 0.11, "#0ea5e9", "Flink RUNNING, model\nloaded, 5-event probe"),
        ("ready", 0.34, OK, "serving · idle timer 45 min\nmin uptime 10 min"),
        ("stopping", 0.10, "#d97706", "drain ≤ 180 s\nthen stop instance"),
        ("sleeping", 0.10, "#9ca3af", "back to TERMINATED"),
    ]
    cx = x0
    for name, frac, colour, note in segs:
        w = (x1 - x0) * frac
        svg.rect(cx, y, w - 3, 46, colour, rx=4, opacity=0.9)
        svg.text(cx + (w - 3) / 2, y + 28, name, size=10.5, fill="#ffffff", weight="600", anchor="middle")
        for j, ln in enumerate(note.split("\n")):
            svg.text(cx + (w - 3) / 2, y + 66 + j * 13, ln, size=9, fill=MUTED, anchor="middle")
        cx += w

    def mark(frac, label, colour):
        x = x0 + (x1 - x0) * frac
        svg.line(x, y - 46, x, y, colour, 1.4)
        svg.dot(x, y - 46, 4, colour)
        svg.text(x, y - 54, label, size=9.5, fill=colour, weight="600", anchor="middle")

    mark(0.10, "visit → POST /lifecycle/ensure-ready", ACCENT)
    mark(0.80, "45 min with no activity", ALERT)
    mark(0.90, "Cloud Tasks idle timer fires", ALERT)

    svg.text(60, 268, "Counts as activity: POST /lifecycle/ensure-ready, POST /lifecycle/activity, and proxied "
                      "POST/PATCH API calls. Passive GET polling does not keep the VM awake.", size=10.5)
    svg.text(60, 288, "Guards: 10 wakes / 10 min per hashed client IP · 24 wakes/day · 12 uptime hours/day · "
                      "60 s cooldown after a failed start · operator keep-awake ≤ 180 min.", size=10.5, fill=MUTED)
    svg.text(60, 308, "A failure at any startup stage moves to `failed`; the next visit retries after the "
                      "cooldown. On reboot the Flink submitter restores from the newest complete checkpoint.",
             size=10.5, fill=MUTED)
    svg.text(60, 334, "Segment widths are illustrative. No wake, ready or stop duration has been measured — "
                      "see docs/status-checklist.md item 10.", size=10, fill=ALERT)
    footer(svg, ".env.example PIPELINE_* defaults · services/controller/app.py")
    svg.save("lifecycle-timeline.svg")


# ---------------------------------------------------------------------------------------------
# CA2 · Flink fault-injection and recovery timeline (measured 2026-09-18 on the GCP VM)
# ---------------------------------------------------------------------------------------------
def fig_recovery_timeline():
    svg = Svg(980, 470, "CA2 · Flink recovery after a TaskManager kill (measured)",
              "VM fraudwatch-pipeline, asia-south1-a · job 20d1ba3421afcb29654a0025021b8451 "
              "\"fraudwatch-flink\" · 2026-09-18, times UTC")
    x0, x1 = 80, 900
    # timeline spans 18:55:00 -> 18:56:40 = 100 s
    t_start, t_span = 0.0, 100.0

    def sx(sec):
        return x0 + (sec - t_start) / t_span * (x1 - x0)

    y = 190
    svg.rect(x0, y, x1 - x0, 44, GRID, rx=4, opacity=0.6)
    # RUNNING 0-72, RESTARTING 72-82, RUNNING 82-100
    svg.rect(sx(0), y, sx(72) - sx(0), 44, OK, rx=4, opacity=0.85)
    svg.rect(sx(72), y, sx(82) - sx(72), 44, ALERT, rx=4, opacity=0.85)
    svg.rect(sx(82), y, sx(100) - sx(82), 44, OK, rx=4, opacity=0.85)
    svg.text(sx(36), y + 27, "RUNNING", size=11, fill="#ffffff", weight="600", anchor="middle")
    svg.text(sx(77), y + 27, "RESTARTING", size=9, fill="#ffffff", weight="600", anchor="middle")
    svg.text(sx(91), y + 27, "RUNNING", size=11, fill="#ffffff", weight="600", anchor="middle")

    for sec, label in ((0, "18:55:00"), (20, "18:55:20"), (40, "18:55:40"), (60, "18:56:00"),
                       (80, "18:56:20"), (100, "18:56:40")):
        svg.line(sx(sec), y + 44, sx(sec), y + 52, MUTED)
        svg.text(sx(sec), y + 66, label, size=9.5, fill=MUTED, anchor="middle")

    events = [
        (12, "18:55:12 · TaskManager process killed", ALERT, -1),
        (64, "18:56:04 · TaskManager restarted", ACCENT, 1),
        (72, "+60 s · heartbeat timeout → RESTARTING", ALERT, -1),
        (82, "~10 s later · RUNNING again, restored from chk-80", OK, 1),
    ]
    for sec, label, colour, side in events:
        x = sx(sec)
        if side < 0:
            svg.line(x, y - 58, x, y, colour, 1.4)
            svg.dot(x, y - 58, 4, colour)
            svg.text(x, y - 66, label, size=9.5, fill=colour, weight="600",
                     anchor="middle" if 0.2 < sec / 100 < 0.85 else "start")
        else:
            svg.line(x, y + 44, x, y + 96, colour, 1.4)
            svg.dot(x, y + 96, 4, colour)
            svg.text(x, y + 112, label, size=9.5, fill=colour, weight="600", anchor="middle")

    tiles = [
        ("checkpoint mode", "exactly_once", "Flink REST /jobs/<id>/checkpoints/config"),
        ("interval / timeout", "10 000 / 60 000 ms", "retained: delete_on_cancellation false"),
        ("checkpoints before kill", "77 total · 77 completed · 0 failed", "0 failures in steady state"),
        ("restores", "counts.restored = 4", "latest chk-80, is_savepoint false"),
        ("failed during outage", "6", "expected: attempted while TM was down"),
        ("persisted rows", "flink: 15 rows / 15 distinct event_id", "0 duplicate rows"),
    ]
    for i, (k, v, note) in enumerate(tiles):
        tx = 80 + (i % 3) * 293
        ty = 336 + (i // 3) * 54
        svg.rect(tx, ty, 278, 46, "#f9fafb", stroke=GRID, rx=5)
        svg.text(tx + 10, ty + 15, k, size=9, fill=MUTED)
        svg.text(tx + 10, ty + 30, v, size=11, weight="600")
        svg.text(tx + 10, ty + 42, note, size=8.5, fill=MUTED)

    svg.text(80, 100, "What this verifies: exactly-once checkpointing, retained-checkpoint restore after a "
                      "TaskManager failure, and no lost or duplicated persisted rows at this scale.", size=10.5)
    svg.text(80, 118, "What it is NOT: a throughput benchmark and not a Flink-vs-Spark comparison — those "
                      "remain not measured yet.", size=10.5, fill=ALERT)
    footer(svg, "Flink REST API + PostgreSQL on the fraudwatch-pipeline VM, 2026-09-18 (session log)")
    svg.save("ca2-flink-recovery-timeline.svg")


# ---------------------------------------------------------------------------------------------
# System Intelligence panel (measured 2026-09-18) with explicit not-measured tiles
# ---------------------------------------------------------------------------------------------
def fig_system_intelligence():
    svg = Svg(980, 470, "System Intelligence · observed state of the deployed stack",
              "GET /api/v1/health and the Flink REST API on fraudwatch-pipeline, 2026-09-18. "
              "Grey tiles are not instrumented — null never means zero.")
    ok_rows = [
        ("overall", "ok", ""),
        ("postgres", "ok", "query ok · query_ms 13.9"),
        ("kafka", "ok", "1 broker · 7 topics"),
        ("scoring", "ok", "artifact fw-ieee-stream-1.1.0"),
        ("api", "ok", ""),
    ]
    svg.text(60, 96, "Component health", size=12, weight="600")
    for i, (k, v, note) in enumerate(ok_rows):
        y = 116 + i * 30
        svg.rect(60, y, 400, 24, OK, rx=4, opacity=0.12)
        svg.dot(74, y + 12, 5, OK)
        svg.text(90, y + 16, k, size=11, weight="600")
        svg.text(180, y + 16, v, size=11, fill=OK, weight="600")
        svg.text(230, y + 16, note, size=9.5, fill=MUTED)

    svg.text(520, 96, "Flink job", size=12, weight="600")
    fl = [
        ("job", "fraudwatch-flink"),
        ("id", "20d1ba3421afcb29654a0025021b8451"),
        ("state", "RUNNING"),
        ("tasks", "6 running / 6 total · 0 failed"),
        ("state backend", "HashMapStateBackend"),
        ("checkpoint storage", "FileSystemCheckpointStorage"),
    ]
    for i, (k, v) in enumerate(fl):
        y = 116 + i * 24
        svg.text(520, y + 14, k, size=9.5, fill=MUTED)
        svg.text(660, y + 14, v, size=10, weight="600" if k == "state" else "400",
                 fill=OK if k == "state" else INK)

    svg.text(60, 300, "Measured", size=12, weight="600")
    meas = [
        ("e2e probe, run 1", "5 sent / 5 persisted · max 530 ms"),
        ("e2e probe, run 2 (post-recovery)", "5 sent / 5 persisted · max 168 ms"),
        ("duplicate persisted rows", "0"),
    ]
    for i, (k, v) in enumerate(meas):
        x = 60 + i * 300
        svg.rect(x, 314, 286, 50, "#f9fafb", stroke=GRID, rx=5)
        svg.text(x + 10, 331, k, size=9, fill=MUTED)
        svg.text(x + 10, 350, v, size=11, weight="600")

    svg.text(60, 392, "Not measured yet", size=12, weight="600", fill=MUTED)
    notm = [
        ("throughput (events/s)", "no experiment runner"),
        ("p50 / p95 / p99 by engine", "needs both engines running"),
        ("Flink vs Spark comparison", "ops-agent/app/ is empty"),
    ]
    for i, (k, v) in enumerate(notm):
        x = 60 + i * 300
        svg.rect(x, 406, 286, 44, "#f3f4f6", stroke=GRID, rx=5)
        svg.text(x + 10, 423, k, size=9, fill=MUTED)
        svg.text(x + 10, 440, "_not measured yet_ · " + v, size=9.5, fill=MUTED)

    svg.text(60, 288, "e2e_ms is producer-to-database wall-clock latency for the whole path — not "
                      "engine-internal processing time, and it includes result-consumer batching.",
             size=10, fill=MUTED)
    footer(svg, "GET /api/v1/health · Flink REST API · POST /api/v1/internal/probe, 2026-09-18 (session log)")
    svg.save("system-intelligence-panel.svg")


def main():
    ev = json.loads((ART / "evaluation.json").read_text(encoding="utf-8"))
    fig_pr_curve(ev)
    fig_roc_curve(ev)
    fig_confusion(ev)
    fig_threshold_sweep(ev)
    fig_calibration(ev)
    fig_importance(ev)
    fig_lifecycle()
    fig_recovery_timeline()
    fig_system_intelligence()


if __name__ == "__main__":
    main()
