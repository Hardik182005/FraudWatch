# FraudWatch — Visualization and Diagram Upgrade Prompt

## Task

Upgrade the existing FraudWatch Live and FraudWatch Lab websites with clear, polished, useful visualizations wherever they improve understanding. Implement the changes in the existing project; do not stop at a design proposal or create a disconnected prototype.

Replace text-heavy explanations, plain metric boxes, and unused whitespace with the appropriate charts, diagrams, timelines, or interactive evidence views. Keep short explanations where they help. Do not turn every number into a chart or replace useful tables with decorative graphics.

Preserve the working backend, two-site synchronization, LightGBM scoring, actual Flink/Spark execution, automatic cloud wake/idle shutdown, and ₹0 out-of-pocket constraint. Keep the Viva tab/page removed. This prompt changes presentation and the instrumentation needed to support it; it does not authorize paid services or a replacement architecture.

## 1. Visual direction

Follow the supplied Polar Sentinel reference: bold clean typography, white surfaces, subtle grid lines, strong black buttons, generous spacing, and large purposeful diagrams. Use a consistent shared design system across both websites.

- Main text should be comfortably readable, with clear labels and restrained secondary metadata.
- Use a consistent visual identity for Flink and Spark across diagrams, charts, legends, and tables.
- Use distinct labelled styles for ordinary activity, review, flagged activity, errors, and unknown state. Do not rely on colour alone.
- Prefer flat, precise 2D charts for quantitative comparisons. Reserve interactive 3D for landing-page architecture and spatial exploration; do not make confusion matrices, bars, or pie charts 3D.
- Avoid excessive chart borders, repetitive cards, gratuitous animation, rainbow palettes, meaningless gauges, and oversized empty chart panels.
- Keep the dashboard usable without WebGL. Support responsive layouts, dark/light themes, keyboard focus, screen-reader summaries, and reduced motion.
- Use short chart titles that state what is measured. Include units, time range, data source, and a useful legend where needed.

## 2. Data and interaction rules for every visual

1. Identify whether the visual shows **Live measurements**, **Saved run**, **Held-out evaluation**, **Validation exploration**, or **Illustration**. Show that distinction near the visual.
2. Use actual backend records and evaluation artifacts. Never invent chart points, successful stages, feature importance, health status, model scores, benchmark numbers, or missing timestamps.
3. Distinguish zero from unavailable, not instrumented, stale, and not applicable. Empty visuals need a useful action such as selecting a run or starting a permitted experiment.
4. Keep session, run, model, threshold, time range, and engine filters synchronized where their meanings match. Do not apply live-run filters to an unrelated held-out evaluation.
5. Provide hover and keyboard-accessible details. Use selection to reveal evidence, not just enlarge a decorative shape.
6. Add a data-table alternative and CSV export for relevant charts. Provide readable SVG/PNG export for report figures where feasible.
7. Use honest axes and scales. Bar charts should normally start at zero; label any logarithmic scale. Do not smooth away spikes or interpolate through missing measurements without disclosing it.
8. Bound live history, paginate large tables, and downsample large series with a documented method that preserves important anomalies. Display aggregation intervals.
9. Do not show confidence intervals, percentiles, or trends without sufficient underlying observations and a defined calculation.
10. Use permitted demo data and sanitize tooltips, labels, logs, and exports. Backend authorization applies to drill-downs as well as top-level pages.

## 3. Visualization map

Implement the following where the corresponding functionality exists. Add necessary instrumentation when it is part of the required working feature; otherwise label the missing evidence and report the blocker instead of fabricating a visual.

| Location | Primary visual | What the user should understand or do |
|---|---|---|
| Live landing | Interactive 3D transaction pipeline | See how a transaction becomes a score and review decision |
| Lab landing | Distinct 3D parallel Flink/Spark pipeline | Understand that the same input can take two processing paths |
| Live console | Throughput timeline plus decision breakdown | Observe processing activity and actual review outcomes |
| Transaction inspector | Event journey timeline and feature evidence | Understand this transaction's processing and decision |
| Scenario library | Compact sequence previews and run progress | Understand and execute each preset |
| CA1 evaluation | Confusion-matrix heatmap and precision-recall curve | Assess fraud detection and its tradeoffs |
| CA1 model evidence | Feature contribution chart | Understand how supported features influenced a prediction |
| CA2 workspace | Parallel processing timeline and matched-event comparison | Compare actual Flink and Spark behaviour |
| Late-event experiment | Event-time/arrival-time diagram | Understand order, watermark, and late-data handling |
| Recovery experiment | Checkpoint and restart timeline | See what failed, recovered, and was preserved |
| System Intelligence | Service topology, correlated traces, and structured logs | Find the cause of a delay, failure, or missing result |
| Cloud lifecycle | Wake/ready/drain/sleep timeline | Understand readiness and when resources are running |
| Report/evidence | Exported figures with captions and provenance | Present results without inventing or manually retyping numbers |

## 4. Both landing pages

### FraudWatch Live

Use a split hero with concise copy on the left and a large interactive 3D scene on the right:

`Source → Kafka → Flink → LightGBM → Review queue`

- Make nodes selectable with readable labels and compact explanations.
- Provide rotate, zoom, reset, and 2D/3D controls.
- Show an event's actual route when a permitted live run is selected.
- When no run exists, use a clearly labelled architectural illustration. Illustrative particles must not be presented as measured transactions.
- Keep **Launch live demo** visible and link directly to the existing working console.

### FraudWatch Lab

Use a different hero composition showing a shared input splitting into Flink and Spark lanes and converging at results.

- Explain the configured processing modes visually without implying a measured speed advantage from decorative animation.
- Link directly to the executable CA2 workspace and CA1 evaluation.
- Keep deep technical details out of the hero; place them in selected-node panels and expandable sections.

Lazy-load both scenes, pause offscreen rendering, and provide an accessible 2D node list/diagram. Verify label placement on actual desktop and mobile screenshots.

## 5. Live console and alerts

### Activity overview

- Show generated, accepted, processed, and persisted event counts with explicit definitions.
- Add a bounded time-series chart for offered and processed events per second, using the same interval and units.
- Show review-policy outcomes as a stacked bar or compact distribution with absolute counts. Label these as decisions, not ground-truth fraud rates.
- Show a risk-score distribution with the applicable model threshold marked. Identify scores as model scores unless probability calibration is established.
- Display measured latency distributions or percentiles with the exact timing boundary and sample count.

Keep the transaction table as the precise inspection surface. Link chart selections to filtered rows without changing the selected run unexpectedly.

### Alert detail

Combine a short decision summary with:

1. A transaction journey: generated, broker acknowledged, processed, scored, persisted, and displayed, as actually instrumented.
2. Model evidence and rule evidence in distinct areas.
3. A compact account activity timeline where account history genuinely exists.
4. A review/audit timeline for recorded human decisions.

Show missing spans as missing, retries as retries, and unknown truth as unknown. A predicted alert is not proof that fraud occurred.

## 6. Preset scenario visuals

Give each existing scenario a short sequence preview, such as repeated small payments followed by a large payment. Label that preview as an illustration.

After **Run preset**, replace the preview's emphasis with actual execution:

- Seed/configuration and run identifier.
- Lifecycle progress and event counts.
- Actual account/event timeline.
- Detected conditions, model scores, rule triggers, and final decisions.
- Expected conditions beside observed results, including misses or unexpected alerts.

Replay must create or select a traceable run and must not animate old results while pretending to execute new transactions. Do not hardcode a successful fraud detection because of the scenario's name.

## 7. CA1: a proper model-evaluation workspace

### Confusion matrix

Replace the four plain boxes with a polished 2×2 heatmap:

- Rows: **Actual ordinary**, **Actual fraud**.
- Columns: **Predicted ordinary**, **Predicted fraud**.
- Cells: TN top-left, FP top-right, FN bottom-left, TP bottom-right.
- Display large counts and understandable descriptions: correctly ordinary, false alert, missed fraud, and correctly flagged fraud.
- Use a sequential colour scale, numeric legend, readable contrast, and deliberate spacing.
- Offer **Counts** and **Percentage of actual class** views. Normalize percentages by row; show N/A for an empty row.
- Keep raw counts in accessible details and provide a table alternative.
- Display evaluated sample count, class support, model version, dataset/split, threshold, evaluation date, and artifact reference.
- Place precision, recall, and F1 alongside the matrix, calculated from the same predictions.
- Verify all cells sum to the evaluated sample count and the displayed metrics reconcile.
- If authorized labelled examples exist, selecting a cell opens the matching filtered examples. Otherwise show an aggregate explanation without a fake drill-down.

Live predictions without ground-truth labels cannot produce an evaluation confusion matrix. Do not treat a model output, rule label, or reviewer choice as verified ground truth.

### Supporting evaluation charts

Use a focused set of useful charts rather than rendering every metric by default:

- **Class-balance bar:** ordinary versus fraud support for the displayed split.
- **Precision-recall curve:** primary ranking view for the imbalanced classification problem, with an appropriate labelled prevalence reference and operating point.
- **Threshold tradeoff:** precision/recall across validation thresholds, with sample support and resulting alert volume where available.
- **Model comparison:** aligned baseline-versus-LightGBM metrics from the same validation split; distinguish validation from final test results.
- **ROC curve:** optional secondary view, with clear labels and actual prediction scores.
- **Calibration plot:** only if computed from appropriate labelled data; do not imply raw LightGBM scores are calibrated probabilities.

Keep the official held-out result fixed to its preselected threshold. Interactive threshold exploration belongs in a clearly labelled validation view. Never tune repeatedly against the final test set to improve the screenshot.

### Model explanations

- If actual TreeSHAP or another supported attribution method is implemented, show a signed feature-contribution bar/waterfall for a selected prediction.
- Explain the output scale and baseline; do not label log-odds contributions as percentage probability changes.
- Use real feature names and available values. Keep categorical encodings understandable.
- A global importance chart must state its method and evaluation population. Global importance is not the explanation for one transaction.
- Present rule evidence separately. Do not rename heuristic reasons as SHAP values or causal explanations.

Preserve honest evaluation numbers even if the model is weak. Investigate performance through valid modelling work, not visual manipulation.

## 8. CA2: live stream-processing visuals

Replace the default wall of concept cards with working controls and runtime evidence.

### Same stream, two engines

- Show parallel Flink and Spark lanes with actual job/query IDs, status, progress, and event counts.
- For a selected event, show recorded timestamps and output availability in both lanes.
- Display Spark batch boundaries only when recorded by the actual query instrumentation.
- Use a matched-event table for precise comparison and a chart for measured delays/distributions.
- Show unmatched, failed, duplicate, and still-pending results alongside successful pairs.
- Use common axes, timing boundaries, and equivalent processing semantics. Do not compare provisional output with finalized output.
- Put workload, warm-up, resource allocation, trigger interval, versions, and sample size in a compact experiment-details panel.

### Event time, watermarks, and late data

- Draw event-time and arrival/processing-time tracks with labelled events.
- Show observed watermarks and window boundaries when instrumented.
- Mark whether an event was accepted, updated a result, quarantined, or excluded according to the implemented policy.
- Offer a controlled late-event action and update the diagram from the resulting records.
- Put an illustrative explanation in a separate labelled mode if the required runtime watermark measurement is unavailable.

### Checkpoints and recovery

- Show checkpoint completion/failure markers, interruption, restart, restoration, and resumed outputs on a timeline.
- Link markers to real checkpoint/job IDs and logs.
- Display the measured recovery interval and any duplicates or missing records, with definitions.
- Keep intentional failure injection protected and isolated. Merely selecting a timeline must never trigger a failure.

## 9. System Intelligence: visual observability

Make the page useful for investigating the real pipeline.

### Service topology

Show the implemented generator, API, Kafka, Flink, Spark, scoring, consumer, and database as connected nodes.

- Node state must come from fresh observations: healthy, degraded, unavailable, idle, stale, or unknown.
- Selecting a node filters related metrics, logs, and incidents.
- Show a compact end-to-end completion indicator separately from individual process health.
- Do not make every node green because containers are running.

### Trace waterfall

Search an event or trace ID and display recorded stages with start times and durations. Correlate traces with the same event's predictions, alerts, retries, and engine results.

Durations must respect clock boundaries. Flag missing timestamps or unsynchronized clocks instead of presenting negative durations as valid performance.

### Metrics and data quality

Use small, purposeful time-series panels for supported observations such as throughput, consumer lag, inference latency, errors, and checkpoint age. Do not invent CPU, memory, or backpressure metrics when they are not collected.

Show malformed, duplicate, late, failed, pending, and persisted records using a reconciliation table or an appropriate flow diagram. Use a Sankey only if categories form a valid conserved flow; do not add overlapping retry counts as if they were unique events.

### Logs remain inspectable

Keep a searchable structured-log table with time, severity, service, run/event IDs, and message. Add a compact error-frequency timeline above it if useful.

Support live-tail pause, historical filters, pagination, related-trace links, and sanitized exports. Restrict operators' infrastructure logs appropriately. A colourful log chart must not replace access to the actual relevant log entries.

## 10. Cloud lifecycle and resource use

Visualize the actual runtime lifecycle:

`Sleeping → Starting VM → Starting services → Restoring processing → Ready → Draining → Sleeping`

- Connect state changes to the durable wake/shutdown controller.
- Show current stage, observed elapsed time, and recent lifecycle events.
- While starting, use an indeterminate progress indicator unless genuine progress percentages exist. Never invent a loading percentage or completion time.
- Show the configured idle policy and last meaningful activity. A displayed countdown is an estimate and must be reconciled with active jobs and controller state.
- Separate VM running time from Cloud Run request activity and persistent storage.
- Any credit/cost figure must be identified as an estimate or an actual billing observation, with its source, rate assumptions, update time, and currency. Do not claim a stopped VM means zero total cost.

## 11. Architecture and report figures

Create consistent, exportable diagrams for:

1. End-to-end processing architecture.
2. The two websites and shared backend/state.
3. Model training versus live inference.
4. Flink/Spark experiment data paths.
5. Automatic cloud wake and idle shutdown.

Use labelled arrows with meaningful direction and avoid crossing lines where possible. Distinguish data flow, control requests, and persistence. Show only implemented components; mark planned or blocked components explicitly in internal documentation.

Keep diagrams concise and renderable on an A4 report page. Generate report charts from the same saved results used by the websites and include readable titles, captions, units, run/model references, and dates.

## 12. Implementation and completion checks

- Inspect existing components and chart libraries before adding dependencies. Prefer a small shared visualization layer over a different library for every chart.
- Reuse typed data contracts and consistent metric definitions. Add missing backend aggregates only where required, without exposing secrets or unrestricted queries.
- Deliver page by page with actual rendered inspection. Check desktop, mobile, dark/light themes, zoomed text, empty data, stale data, failures, and reduced motion.
- Test confusion-matrix calculations, row normalization, zero denominators, filter consistency, event/run linking, and graph/chart data against saved evidence.
- Verify that selecting chart marks opens the correct permitted records and exports match the selected dataset/run.
- Ensure the visual layer does not slow event processing or make the console unusable with large runs.
- Update screenshots and report figures affected by the changes.

Return the updated working project, screenshots of the key views, a concise list of visuals added, their real data sources, test results, and any instrumentation gaps. Do not claim this is complete if only decorative diagrams work while scenarios, results, or logs are disconnected.
