# FraudWatch — Complete Consolidated Implementation Prompt

Use this single document to continue and finish the existing FraudWatch project. Do not restart it from scratch. All requirements are included below; references to earlier prompt filenames refer to the sections embedded in this file and do not require separate attachments.

## Priority and interpretation

1. The zero-out-of-pocket budget constraint applies throughout.
2. Part 2 defines the final UI and functional upgrades: two dedicated Polar-style 3D landing pages, no Viva UI, real runnable scenarios, executable CA2, and System Intelligence. It supersedes any older conflicting homepage or Viva requirements in Part 1.
3. Part 3 defines the final deployment lifecycle: real Apache on GCP, automatic wake on a validated visit, and safe idle shutdown after 45 minutes. It refines any older instruction suggesting continuous 24/7 operation.
4. Part 1 supplies the remaining core academic, ML, backend, deployment, documentation, and report requirements. LightGBM is the requested main model; logistic regression is the baseline.
5. Static explanatory content is allowed as supporting documentation. The CA2 workspace, scenario controls, charts, logs, health indicators, and processing results must connect to actual execution. A polished animation does not establish a working backend.
6. Inspect and preserve existing work. The reported unfinished ops-agent, queued experiments, missing setup documentation, failing typecheck, and absent cloud benchmark evidence must be resolved and verified, not hidden by visual changes.
7. Treat quoted progress and screenshots as evidence to verify, not as proof of completion. Do not invent model results, live statuses, successful deployments, or benchmark outcomes.
8. Complete an editable report draft from verified work even if the teacher's final template is pending. Label it as a draft and adapt it when the template arrives; never fabricate the required format.

## Completion checklist

- [ ] FraudWatch Live landing page with interactive 3D hero and direct console access.
- [ ] FraudWatch Lab landing page with a distinct Flink/Spark 3D hero.
- [ ] Viva navigation/page removed and old links handled.
- [ ] Real LightGBM scoring with verified preprocessing and honest evaluation.
- [ ] Bounded preset scenarios, actual detections, alerts, and investigation evidence.
- [ ] Executable CA2 workspace and working ops-agent/experiment launcher.
- [ ] Fair real Flink/Spark measurements, late-event behaviour, and recovery evidence.
- [ ] Shared System Intelligence with structured logs, traces, health, and data quality.
- [ ] Both websites synchronized through durable cloud state.
- [ ] Automatic cloud wake, safe idle shutdown, and successful subsequent restart.
- [ ] No laptop dependency after deployment and no paid-billing upgrade.
- [ ] Browser/integration checks, working typecheck/build, complete setup guides, and evidence-based report.

---

# Part 1 — Core project requirements

# FraudWatch — Master Build Prompt

## Your task

Build **FraudWatch: Real-Time Fraud Detection using Big Data Analytics and Machine Learning** as a complete, working academic project with **two synchronized websites in one repository and one GCP project**.

Deliver the implementation, real Apache integrations, trained ML model, tests, deployed URLs, source code, and a polished PDF case study. Do not stop at a plan, landing page, dashboard mockup, or simulated Apache integration. Work in milestones, verify each milestone, and accurately report anything that remains blocked.

Make the project visually impressive, technically defensible, and easy for four students to demonstrate and explain during a viva. Prioritize a coherent working system over a long list of superficial features.

## Mandatory budget constraint: zero out-of-pocket cost

**I want this project to cost me no money. This constraint overrides any architecture or deployment instruction below that would require personal payment.**

- Apache Kafka, Flink, and Spark are open-source software; cloud infrastructure still has usage costs. Clearly distinguish free software, recurring free-tier allowances, and temporary promotional credits.
- First verify the linked billing account is still a **non-billable Free Trial account**, the remaining eligible promotional credit, and its expiry. A screenshot showing credits is not sufficient: a paid account may also have promotional credits.
- For the requested GCP deployment, use only eligible services covered by verified existing Free Trial credits and applicable free allowances. Explain the estimated credit consumption as well as the expected out-of-pocket amount of zero under the verified trial conditions. Do not describe credit-funded infrastructure as permanently free.
- Never activate, upgrade, or switch to paid billing; never purchase credits, subscriptions, domains, reservations, or paid Marketplace software. Do not offer a paid-spend approval as the default path.
- If the account is already paid, its status cannot be verified, credits are unavailable/insufficient, or the required service is excluded from trial coverage, do not provision chargeable infrastructure. Continue the complete local implementation and document that zero-cost cloud deployment remains blocked. Do not silently replace the requested cloud Apache backend with a simulation or laptop dependency.
- Size and test an economical cloud profile. Start Spark comparison jobs only when required. Preserve the real cloud Flink pipeline during agreed demonstration periods; provide explicit start/stop controls and show offline status outside those periods.
- Account for storage, disks, public IPs, networking, image registries, build services, logging, databases, and secrets as well as VM compute. Stopping a VM does not automatically remove every associated resource cost.
- Save the source, report, model, and evidence outside the trial resources before expiry. Explain that an unupgraded trial ends when time or credit runs out and that the cloud deployment then stops.
- Do not assume the recurring GCP free tier can host this entire multi-service Apache stack indefinitely. If permanent zero-cost independent cloud hosting is required, establish a verified feasible alternative or state the constraint honestly; do not promise one without evidence.
- Optional OpenAI, Groq, and Gemini integrations must remain disabled by default. Enable a provider only after verifying an applicable no-charge allowance and preventing paid fallback; otherwise retain the deterministic assistant. Possession of an API key does not establish free usage, and GCP trial credit does not automatically cover external API charges.

## Academic context

- **Group:** 4
- **Members:** Hardik Hinduja, Avinash Gehi, Siddharth Dey, Sahil Deshmukh
- **CA1 mini-project:** Real-Time Fraud Detection using Big Data Analytics and Machine Learning
- **CA2 content beyond syllabus:** Apache Flink: True Stream Processing and Comparison with Spark Micro-Batch
- **Submission date:** Monday, 21 September 2026
- **Viva:** Based on the submitted case study

CA2 must extend the actual CA1 pipeline. The two assessments must form one connected project. The teacher's final submission format is pending, so produce a well-structured report that can be adapted without rewriting the project. Do not invent institution details, roll numbers, signatures, approvals, completed contributions, or experimental results.

## Known GCP details and access

- **Project display name:** FraudWatch
- **Project ID:** `fraudwatch-508912`
- Prefer `asia-south1` if the required services, quota, and cost make it appropriate; verify before deployment.
- The supplied screenshot identifies the project; it does not provide credentials, IAM permissions, a service account, or a verified current budget.
- Inspect available authenticated GCP access and existing resources before creating anything. Reuse suitable existing resources and preserve unrelated resources.
- Do not infer a project number, billing account, custom domain, or secret values from screenshots. Do not upgrade billing or assume the entire trial credit is available to spend.
- Follow the zero-out-of-pocket constraint above. Document expected trial-credit consumption, what stays on, and how to stop it; continue local implementation while billing eligibility or access is unresolved. Never interpret this project request as permission to charge a payment method.

## Two websites, one shared system

### Website 1: FraudWatch Live

An operational fraud-monitoring demo. Use the deployment/service name `fraudwatch-live` where available.

The root `/` must be a dedicated Polar Sentinel-inspired landing page with bold typography, a subtle grid, a large interactive 3D transaction-pipeline hero, and a clear **Launch live demo** action leading to `/console`. Keep copy concise and provide direct console links. The console itself must open on controls and real results without a marketing hero.

Essential views:

1. **Live overview:** transaction stream, scored transactions, flagged count, observed processing delay, throughput, and actual service health.
2. **Transaction inspector:** transaction details, model score, rule evidence, contextual features, model version, and event timeline.
3. **Alert investigation:** filter alerts, inspect evidence, acknowledge an alert, and record a demo review decision with an audit trail. Review decisions must not silently retrain the model.
4. **Scenario controls:** start, pause, reset, choose a reproducible scenario, adjust supported event rate, and inject a suspicious pattern.
5. **Visual pipeline:** an interactive 3D architecture view with a usable 2D alternative.

Show INR amounts with appropriate formatting. Use synthetic account identifiers and sample merchants. Distinguish transaction amount flagged for review from confirmed fraud losses or money saved. This is an educational system, not a live banking integration or real payment-blocking service.

### Website 2: FraudWatch Lab

An interactive academic companion containing the complete CA1 and CA2 work. Use the deployment/service name `fraudwatch-lab` where available. Its root `/` must be a dedicated landing page with a distinct interactive 3D Flink-versus-Spark hero and direct actions to open the executable CA2 lab and CA1 model workspace.

Organize it into concise sections or tabs:

- **CA1:** problem, dataset, preprocessing, model, results, and connection to the live demo.
- **CA2:** Flink implementation, event time, watermarks, state, checkpoints, recovery, and the Spark comparison.
- **Experiment lab:** start or inspect authorized benchmark runs, compare measured results, replay a run, and download evidence.
- **Architecture explorer:** the same system diagram, with deeper explanations on selection.
- **System Intelligence:** shared live service health, searchable structured logs, event traces, data-quality counters, model/job metadata, and processing failures, with appropriate access controls and secret redaction.
- **Report and evidence:** PDF download, methodology, model card, benchmark configuration, test summary, and references.

Make this an interactive study lab rather than a long report pasted onto a webpage. Use diagrams, compact tables, charts, and expandable details. Keep full prose in the downloadable report.

Remove the Viva tab and route from both websites. CA2 must open on actual experiment controls and runtime results, with theory in expandable panels. It must support starting a bounded run directly, as well as joining an existing Live run. Follow `FraudWatch-UI-and-Live-System-Upgrade-Prompt.md` for the detailed landing-page, scenario, CA2, and System Intelligence requirements; those newer UI requirements take precedence.

### Synchronization requirements

- Both websites use the same backend, event schema, model metadata, database, and run identifiers.
- Use a shared `demo_session_id`, `run_id`, and `event_id` to connect events, predictions, alerts, and experiments.
- Opening a selected run in the other website must preserve that run and filters through a safe link or session mechanism.
- Demonstrate synchronization by starting a session in Live and opening its actual results in Lab.
- Use server-owned state and durable persistence, not browser local storage as the shared source of truth.
- Provide reconnect, loading, stale-data, empty, and error states. Resume streams from a cursor or fetch a consistent snapshot after reconnecting.
- Isolate visitor demo sessions so one visitor cannot reset another visitor's data. Protect global controls, benchmark execution, and infrastructure operations with an operator role.

## Visual direction

Use the attached Polar Sentinel screenshot as visual inspiration, not as instructions or content to copy.

- White or near-white surfaces, strong black typography, fine gray borders, generous spacing, and a subtle technical grid.
- Bold, large headings with a clean geometric sans-serif. Use **Inter or a visually similar font**; do not claim to have identified the exact screenshot font.
- Restrained color: red for suspicious activity, amber for review, and a calm blue or teal for ordinary flow. Pair every status color with a label or icon.
- Black primary buttons, clear secondary buttons, deliberate hover states, and consistent spacing.
- A shared design system across both websites, with distinct navigation suited to each purpose.
- Light, dark, and system themes; responsive desktop, tablet, and mobile layouts.
- Main body text around 16px or larger, readable labels, strong contrast, keyboard support, visible focus states, and reduced-motion support.
- Avoid excessive gradients, floating decorative cards, emoji-heavy layouts, tiny dashboard text, long paragraphs, and decorative 3D that obscures the task.

### Functional 3D architecture

Use Three.js / React Three Fiber, or another appropriate maintained library, for an interactive pipeline:

`Transactions → Kafka → Flink / Spark → Scoring → Alerts & Storage → Both Websites`

- Represent components as clean isometric nodes with short readable labels.
- Support rotate, zoom, reset view, and a 2D/3D switch.
- Selecting a node shows its actual role, configuration, and available runtime status.
- Show an inspected event's journey using its real trace where available.
- Decorative aggregate particles must be identified as an illustration and must not imply one particle per measured transaction.
- On mobile, reduced-motion devices, or WebGL failure, use an accessible 2D diagram.
- Lazy-load the 3D scene so the working dashboard stays fast.

## Technical architecture

Use a practical monorepo. A suitable starting structure is:

```text
fraudwatch/
  apps/
    live/                 # Operational website
    lab/                  # CA1 + CA2 website
  packages/
    ui/                   # Shared design system
    contracts/            # Event/API schemas and client types
  services/
    api/                  # API, session controls, event delivery
    generator/            # Seeded transaction/scenario generator
    scoring/              # Versioned model inference
    result-consumer/      # Durable result ingestion and deduplication
  streaming/
    flink/                # Actual Flink job
    spark/                # Actual Spark Structured Streaming job
  ml/
    training/
    evaluation/
    model-card/
  infra/
    compose/
    gcp/
  experiments/
  tests/
  docs/
  reports/
  .env.example
  README.md
```

Prefer React + TypeScript for the websites, FastAPI/Python for the API and ML services, PostgreSQL for durable application records, Apache Kafka for the stream, Apache Flink for CA2 streaming, and Apache Spark Structured Streaming for the comparison. Choose compatible maintained versions and pin them, including connector artifacts and the JDK/Python versions. Document justified substitutions.

Keep the smallest deployable architecture that supports the required features. Do not add Kubernetes, multiple databases, or additional brokers merely to increase the technology count.

## CA1: Data and machine learning

### Dataset and reproducibility

- Use a suitable, legally accessible labelled fraud dataset, recording its source, licence, feature meanings, class balance, and limitations.
- Do not describe a small academic dataset as bank-scale production evidence.
- If access is unavailable, implement a seeded synthetic dataset with probabilistic, overlapping class distributions. Label all associated evaluation as synthetic. Avoid defining labels with the exact same rules later used by the detector.
- Keep illustrative transaction scenarios separate from the held-out model test set.
- Do not invent merchant, location, device, or account-history information for a dataset that does not contain it. Synthetic enrichment must be labelled and evaluated separately.
- Split data in a way appropriate to its time and account structure, fit preprocessing on training data only, and prevent future information and target leakage.

### Training and evaluation

- Use LightGBM as the main requested model and logistic regression as the baseline. Train and compare both using validation evidence, disclose any underperformance, and verify that the deployed artifact is actually LightGBM with matching preprocessing.
- Handle imbalance with an appropriate documented approach. Apply any resampling only to training data.
- Report precision, recall, F1, PR-AUC, ROC-AUC when valid, confusion matrix, class prevalence, and the decision threshold. Explain why accuracy alone is insufficient.
- Select thresholds on validation data; report final held-out metrics separately.
- Version the preprocessing, feature schema, model artifact, seed, dataset reference, and evaluation configuration.
- Treat a score as a model score, not automatically a calibrated probability. Assess calibration before labelling it a probability.
- Export reproducible training and evaluation commands and actual machine-readable results.

### Detection and explanation

Keep **model evidence**, **rule evidence**, and **final review policy** distinct.

Support a compact set of defensible scenarios:

1. Ordinary purchasing behaviour.
2. Rapid transaction bursts from one account.
3. A large deviation from an account's historical spending profile, where history exists.
4. A suspicious sequence of small test transactions followed by a larger transaction.
5. Out-of-order and delayed events for the CA2 demonstration.

Use authentic model explanations where supported, and explicit rule reasons elsewhere. Do not label a rule explanation as a model attribution. A what-if inspector may rescore editable features, but must state that changes illustrate model sensitivity rather than causal conclusions.

## CA2: Actual Apache implementation

### Setup and operations

Provide a documented local container setup with health checks and persistent volumes for:

- Kafka in an appropriate current configuration.
- Flink JobManager and TaskManager, connector dependencies, job submission, checkpoint storage, and restart configuration.
- Spark runtime, the matching Kafka connector, job submission, and checkpoint storage.
- The database, generator, scoring service, API, and result consumer.

Document Docker Desktop/Compose and WSL2 requirements for Windows if applicable. Include host requirements, port mappings, topic creation, startup/shutdown commands, smoke checks, log inspection, troubleshooting, and clean reset instructions that do not delete unrelated data.

Do not assume installing a Python package starts a Flink cluster. Execute the actual jobs and retain job IDs, logs, runtime versions, and output records as evidence.

### Flink implementation

- Consume Kafka transaction events and validate the schema.
- Use event timestamps, a documented watermark strategy, and keyed per-account state where required.
- Implement at least one stateful event-time feature and an explicit policy for late events.
- Expose late-event handling visibly in the Lab using real counters or records.
- Make model inference and sink behaviour observable, including errors and retries.
- Enable checkpoints and demonstrate a controlled worker/job failure followed by recovery in an isolated experiment.
- Use stable record IDs and idempotent result persistence. Explain source, state, and sink guarantees separately.
- Do not claim end-to-end exactly-once processing merely because Flink checkpointing is enabled. External inference and sink effects require their own analysis.

### Spark comparison

- Use actual Spark Structured Streaming in an explicitly configured **micro-batch mode**.
- Compare that configuration with the actual Flink job. Do not claim every Spark streaming mode is micro-batching; describe the pinned version and mode used.
- Feed both engines the same events, schema, model version, equivalent processing logic, and output criteria.
- Use independent consumer groups and distinct output namespaces so both engines process the full input.
- For stateful comparisons, align event-time windows, watermark assumptions, and output/finalization semantics. Do not compare a provisional Flink result with a finalized Spark window.
- Keep model inference resources equivalent and record any shared-service contention. Use isolated sequential trials or balanced resources when concurrent trials would bias results.

### Benchmark methodology

Provide configurable experiment profiles that fit the available hardware. Treat requested event rates as targets until measured.

Record:

- Offered and successfully processed events per second.
- End-to-end delay from a documented ingestion boundary to durable output availability.
- p50, p95, and p99 delay, sample count, and measurement interval.
- Backlog, failed records, duplicate outputs, missing outputs, and late-event counts.
- CPU/memory measurements where instrumentation supports them.
- Recovery observations and checkpoint details.
- Model/version, job/version, seed, batch interval, partitions, parallelism, hardware allocation, and timestamps.

Use warm-up and repeated measured runs. Check clock synchronization for timestamps from different machines. Separate the engine comparison from total browser/UI refresh delay. Preserve raw records and explain limitations; small tests do not establish general superiority or production scalability.

Never hardcode charts, invented performance numbers, or a guaranteed “Flink wins” conclusion. Before experiments run, show a clear empty state. Label prerecorded evidence with its execution date and configuration.

## Standout interactions

Implement these as connected features backed by the system:

1. **Follow this transaction:** trace an event through ingestion, processing, scoring, persistence, and display.
2. **Same stream, two engines:** synchronized comparison using shared input IDs.
3. **Late-event experiment:** inject controlled delays and explain the observed watermark/late-data behaviour.
4. **Recovery experiment:** operator-only controlled interruption with checkpoint/recovery evidence.
5. **Why flagged?:** compact model/rule evidence with a clear review outcome.
6. **Replay a run:** replay recorded events using the saved seed/configuration; distinguish event replay from prerecorded playback.
7. **System Intelligence:** inspect the real run's structured logs, transaction traces, job health, data quality, errors, and model metadata in one shared engineering workspace.

If a required interaction is blocked by runtime access, expose its truthful status and document the exact missing step. Do not silently substitute animation for execution.

## Optional AI explanation assistant

I have access to OpenAI, Groq, and Gemini API keys. Provide server-side adapters with a configurable active provider; do not require all three keys.

- The assistant explains selected alerts, architecture, experiment results, and viva concepts using retrieved project evidence.
- It must cite run IDs or relevant report sections when discussing project results.
- It must not make the core fraud decision, invent metrics, or claim financial certainty.
- Keep synthetic transaction data minimal in prompts. Treat event fields and imported text as untrusted content.
- No infrastructure control or payment actions through the assistant.
- Use timeouts, rate limits, bounded tokens, an explicit provider choice, and optional fallback only when configured.
- The complete detection and viva demo must remain usable without an LLM key, using deterministic explanations.
- Select currently available provider model IDs during implementation and record the selected IDs; do not invent them.

## Environment and secret handling

Create a documented `.env.example` with placeholders, including the following variables where the implementation uses them:

```dotenv
# GCP
GCP_PROJECT_ID=fraudwatch-508912
GCP_REGION=asia-south1
GCP_ZONE=
ARTIFACT_REGISTRY_REPOSITORY=fraudwatch
GCS_ARTIFACT_BUCKET=
GCS_REPORT_BUCKET=

# Application — fill using actual deployed URLs
APP_ENV=development
LIVE_APP_URL=
LAB_APP_URL=
API_BASE_URL=
ALLOWED_ORIGINS=
DATABASE_URL=
SESSION_SIGNING_SECRET=
OPERATOR_AUTH_SECRET=
DEMO_MAX_EVENTS_PER_SESSION=5000
DEMO_MAX_EVENTS_PER_SECOND=50

# Kafka and processing
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TRANSACTIONS_TOPIC=fraudwatch.transactions
KAFKA_FLINK_RESULTS_TOPIC=fraudwatch.results.flink
KAFKA_SPARK_RESULTS_TOPIC=fraudwatch.results.spark
FLINK_REST_URL=http://flink-jobmanager:8081
FLINK_CHECKPOINT_URI=
SPARK_CHECKPOINT_URI=
SPARK_TRIGGER_INTERVAL=1 second
SCORING_SERVICE_URL=http://scoring:8001
MODEL_ARTIFACT_PATH=
MODEL_VERSION=

# Optional AI providers — server-side only
AI_PROVIDER=none
OPENAI_API_KEY=
OPENAI_MODEL=
GROQ_API_KEY=
GROQ_MODEL=
GEMINI_API_KEY=
GEMINI_MODEL=
AI_FALLBACK_ENABLED=false
AI_MAX_OUTPUT_TOKENS=600

# Resource limits — enforce these in the application
MAX_CONCURRENT_BENCHMARKS=1
```

These are configuration examples, not values to pretend are already configured. Explain which settings are public versus private. Keep database/broker/admin endpoints private. Public frontend variables may contain only safe public configuration.

Use Secret Manager for deployed secrets and an ignored local environment file for development. Never put keys in frontend code, Git history, logs, screenshots, the report, or downloadable archives. Prefer attached least-privilege service accounts and Application Default Credentials over downloaded service-account keys. Do not ask me to paste secrets into chat when a secure local or provider configuration flow is available.

## GCP deployment and URLs

- **Automatic wake and idle shutdown:** follow `FraudWatch-Auto-Start-and-Idle-Shutdown-Prompt.md`. Configure appropriate Cloud Run services to scale to zero, and separately implement a durable controller that starts the real Apache VM on a validated website visit and safely stops it after 45 minutes without meaningful activity or valid active jobs. Preserve processing state and evidence. Show startup progress, protect lifecycle controls, prevent duplicate wakes, and verify a full stopped → visit → real processing → idle stop → revisit → recovered processing cycle. Cloud Run autoscaling alone does not stop a Compute Engine VM. This refines availability to automatic on-demand service rather than continuous 24/7 compute.
- **Mandatory: Apache must work after GCP deployment.** Deploy and execute the real Kafka, Flink, and Spark services/jobs on GCP. Both published websites must use this deployed processing pipeline. Local Apache execution alone, prerecorded outputs, browser simulations, or a website that depends on my laptop do not satisfy this requirement.
- The cloud path must be complete: website session controls → deployed generator → cloud Kafka → actual cloud Flink/Spark jobs → scoring → durable cloud results → both websites. Show runtime-backed health and job status; never display a fixed “connected” badge.
- Keep the normal Flink fraud-detection pipeline available while the deployment is running. Spark may run as an operator-triggered cloud comparison job to reduce idle resource use, but it must launch successfully, process the selected stream, persist results, and update the Lab without local commands. Display “idle” truthfully when no Spark experiment is running.
- Package compatible runtime images, connectors, model artifacts, and job artifacts for reproducible cloud execution. Provide automatic startup, service restart policies, health checks, and persistent storage for broker data, application records, and checkpoint/recovery needs. Verify recovery behaviour rather than assuming restart policies preserve state.
- Deploy both websites under the GCP project `fraudwatch-508912` with distinct FraudWatch service names and cross-links.
- Use Cloud Run where appropriate for the websites and stateless API components.
- Run long-lived Kafka/Flink/Spark processes on appropriately sized persistent compute, such as a Compute Engine VM with Docker Compose for this academic deployment. Explain its single-machine availability and resource-contention limitations.
- Do not try to host the complete stateful streaming cluster inside ordinary request-driven Cloud Run services.
- Choose and document secure connectivity between the API and persistent backend. Expose only the necessary application endpoints, not Kafka, databases, Flink administration, or Spark administration to the internet.
- Support authenticated server-to-server requests and narrowly scoped firewall/IAM access.
- For live browser updates, implement SSE or WebSockets with reconnect, backoff, and shared persisted state. Cloud Run connection timeouts and multiple instances must not break synchronization.
- Make the two frontend URLs accessible for the academic demonstration using the agreed audience. Keep operator controls protected and cap public demo usage.
- Cloud Run generates URL suffixes; return the actual verified URLs rather than promising a specific `run.app` address.
- If I own and supply a domain, configure suitable names such as `live.<my-domain>` and `lab.<my-domain>`. Do not buy a domain or invent domain ownership.
- Provide repeatable deploy, update, rollback, and stop instructions. Include resource inventory and cleanup guidance.

## PDF report and academic documentation

Generate one polished, visually verified PDF using the actual finished implementation and measured evidence. Include:

1. Cover with the exact project title, group number, members, and submission date.
2. Abstract, problem statement, objectives, and CA1/CA2 mapping.
3. Architecture diagram and actual technology versions.
4. Dataset provenance, preprocessing, model design, evaluation, and limitations.
5. CA1 implementation and readable screenshots.
6. CA2 Flink implementation, event-time/state/checkpoint explanation, and Spark comparison.
7. Experiment methodology, actual measurements, raw-evidence references, and interpretation.
8. GCP deployment architecture, verified website URLs, and QR codes for those URLs.
9. Testing, failure handling, reproducibility, security, and cost notes relevant to this project.
10. Conclusion, limitations, future work, and official references.
11. Viva preparation with approximately 20 focused questions and short accurate answers.

Include an editable Markdown report source. Keep a proposed team responsibility matrix separate from completed contributions unless each member's work is known. Never claim all features are completed or deployed if they are not.

Also deliver a README, local setup guide, Apache setup guide, GCP deployment guide, model card, benchmark methodology, five-to-seven-minute demo script, and an explicit implementation/status checklist.

## Verification and acceptance criteria

Complete and retain evidence for the following:

- A clean local setup can start the documented services and submit both Apache jobs.
- Model training and evaluation can be reproduced from the documented data and seed.
- Ordinary and suspicious scenarios generate real persisted predictions and alerts.
- Both websites display the same selected session/run, including after refresh and reconnect.
- Flink and Spark process matching input IDs with the expected result semantics.
- Benchmarks contain real samples, validated timestamps, fair configurations, and downloadable raw evidence.
- Late events and controlled recovery behave according to the documented policy.
- Duplicate delivery does not create duplicate logical alerts; missing results are detected and reported.
- Invalid input, unavailable services, and missing optional AI keys produce useful states rather than fake success.
- Public visitors cannot run unbounded workloads, access other private sessions, or trigger infrastructure failures.
- Desktop and mobile UI, keyboard interaction, themes, reduced motion, and 2D fallback are checked.
- Both deployed URLs and cross-site links work, and their backend synchronization is tested after deployment.
- **Cloud-only acceptance test:** with local generators, APIs, brokers, Flink, and Spark stopped, open the deployed websites, start a new cloud session, generate transactions, observe actual Flink predictions/alerts, run the cloud Spark comparison, and confirm matching persisted run IDs in both websites. Retain cloud job IDs, service logs, timestamps, and result counts as evidence. Normal browser access must be the only dependency on the student's computer.
- Verify cloud reconnection and a controlled cloud processing restart using an isolated operator-only experiment. Confirm persisted records and the documented checkpoint/recovery behaviour remain valid. Show a visible unavailable state if cloud processing is stopped.
- The final PDF is rendered and visually inspected; its links, screenshots, numbers, and status match the implementation.
- Build, test, deployment, and evidence logs support the completion claims.

Do not declare success based only on a successful frontend build. If cloud access, quota, secrets, or a dependency blocks a required step, finish the independent work, state the exact blocker, and provide the concrete next action. Clearly distinguish locally verified, cloud verified, prerecorded, illustrative, and not-yet-verified items.

## Working sequence

1. Inspect the workspace, available runtimes, supplied references, and authenticated GCP access. Confirm compatible versions and write a short implementation plan.
2. Build one vertical slice: generator → Kafka → actual processing → scoring → durable result → Live website → matching Lab run.
3. Complete ML evaluation, Flink stateful scenarios, Spark comparison, and experiment evidence.
4. Finish the two dedicated landing pages, shared visual system, interactive diagrams, investigation tools, and System Intelligence workspace; remove the Viva UI.
5. Deploy the two websites and required backend; verify synchronization and operator protection.
6. Produce the report from the final evidence, verify the PDF, and package the reproducible source.

Make routine implementation decisions independently. Ask only for missing information, access, or authorization that materially blocks the next step. Do not substitute planning for implementation or let optional AI polish delay the working CA1/CA2 system.

## Official reference starting points

Verify documentation against the exact versions selected during implementation:

- [Google Cloud Free Trial and Free Tier terms](https://docs.cloud.google.com/free/docs/free-cloud-features)
- [Google Cloud Free Trial billing FAQ](https://cloud.google.com/signup-faqs)
- [Apache Flink Docker deployment](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/standalone/docker/)
- [Apache Spark Structured Streaming](https://spark.apache.org/streaming/)
- [Spark 3.5.6 streaming guide: micro-batch and continuous modes](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) — version-specific background, not a requirement to select this version.
- [Cloud Run WebSocket behaviour and connection considerations](https://docs.cloud.google.com/run/docs/triggering/websockets)

## Final handover

Return the two verified website URLs, PDF report, source archive/repository, exact run/deployment instructions, test and benchmark summary, current resource costs/stop procedure, and any remaining limitations. Make it straightforward for our group to operate, explain, and defend the actual work.


---

# Part 2 — Final landing pages and live-system upgrades

# FraudWatch — Upgrade the Existing UI and Live System

## Task and priority

Upgrade my **existing FraudWatch project**. Implement the changes below; do not merely describe a redesign. Inspect the actual repository and running services first, preserve working functionality and data, and repair incomplete integrations. Do not create another unrelated project or replace functioning backend code with a frontend simulation.

This prompt supersedes earlier requirements about the homepages, Viva UI, model choice, and page layout. Preserve the other requirements from the master prompt, especially the two synchronized websites, real cloud Apache processing, accurate evidence, and **₹0 out-of-pocket spending**.

The current screenshots show text-heavy study pages, a Viva tab I do not want, and CA2 explanations that do not visibly demonstrate execution. Screenshots alone do not prove that the backend is missing: inspect it, identify what actually works, and connect or implement the missing pieces.

The result should feel like a polished fraud-detection product with a useful engineering console. Visual quality and real functionality are equally required.

## 1. Preserve this project context

- Project: **FraudWatch — Real-Time Fraud Detection using Big Data Analytics and Machine Learning**.
- Group 4: Hardik Hinduja, Avinash Gehi, Siddharth Dey, Sahil Deshmukh.
- CA1: fraud detection using a trained **LightGBM** model, with logistic regression as a comparison baseline.
- CA2: actual **Apache Flink** stream processing and a measured comparison with **Spark Structured Streaming in micro-batch mode**.
- GCP project: `fraudwatch-508912`.
- Two applications: **FraudWatch Live** and **FraudWatch Lab**, using one shared backend and consistent session/run/event identifiers.
- A published website must connect to real cloud processing. My laptop must not be required after cloud deployment.

Do not assume a model is correctly trained or deployed because its name appears in the UI. Verify the artifact, preprocessing, feature contract, serving path, metadata, and actual predictions.

## 2. Two dedicated landing pages

Both websites need an intentional landing page at `/`. Their application workspaces must have separate direct URLs. The current study-card homepage is not the finished landing page.

### Shared visual language

Use the earlier **Polar Sentinel** screenshot as the primary visual reference:

- Large, confident black typography with an Inter-like sans-serif font; secondary words may use dark gray.
- White background, fine technical grid lines, restrained borders, generous spacing, and crisp black buttons.
- A desktop split hero: approximately 45% editorial content and 55% interactive 3D scene.
- Make the 3D scene large enough to understand, not a small decorative thumbnail.
- Short supporting copy: one or two sentences. No lengthy academic paragraphs in the hero.
- Restrained teal/blue flow accents and red/amber alert accents, always paired with text or symbols.
- Use consistent typography, spacing, icons, motion, and light/dark/system themes across both websites.
- Avoid repetitive rounded-card grids, excessive gradients, generic finance stock images, fake bank logos, and invented trust badges.
- Provide a polished mobile layout, readable text, keyboard access, visible focus, reduced motion, and a 2D fallback.

### FraudWatch Live landing page

Suggested hero:

**See the transaction.\nCatch the pattern.**

Supporting line: “Follow transactions from arrival to risk score, then inspect the evidence behind every alert.”

Primary action: **Launch live demo** → `/console`.

Secondary action: **Explore the pipeline** → the interactive architecture section or page.

Header: FraudWatch identity, How it works, Scenarios, Open Lab, theme selector, Launch demo.

Create a functional 3D scene showing **Transaction source → Kafka → Flink → LightGBM scoring → Review queue**, using clean architectural blocks, pathways, labels, and restrained animated flow. A selected suspicious event can branch into an alert route after its actual processing result arrives.

The landing scene can run in **Illustration** mode without a session. When attached to a run, show **Live · run ID** and bind observable states to real data. Never imply an illustrative animation is processing real transactions.

Below the hero, include only:

1. A compact scenario preview with an action to open each preset in the console.
2. A three-step visual explanation: stream, score, investigate.
3. A preview of transaction evidence using an explicitly labelled example or a permitted real demo record.
4. A short link to the Lab and project/team footer.

No fabricated metrics, customer counts, accuracy claims, or “money saved” numbers.

### FraudWatch Lab landing page

Suggested hero:

**One stream.\nTwo processing engines.**

Supporting line: “Run the same transactions through Flink and Spark, then inspect timing, state, and recovery.”

Primary action: **Open streaming lab** → `/ca2`.

Secondary action: **Explore fraud detection** → `/ca1`.

Header: FraudWatch Lab identity, CA1, CA2, Experiments, System Intelligence, Open Live, theme selector.

Create a distinct 3D scene: one Kafka source splits into parallel **Flink** and **Spark micro-batch** lanes, which join at a results/measurement area. Explain individual-event flow and batch release with clear labels. Decorative timing must not impersonate a measured performance difference.

Below the hero, include a concise CA1/CA2 visual map, a runnable experiment preview, and access to evidence/report downloads. Move detailed prose into expandable explanations and documentation.

### 3D acceptance

- Use a real interactive 3D renderer with rotate, zoom, reset, node selection, and 2D/3D switching.
- Labels remain legible and do not cover each other at normal viewport sizes.
- Selecting a node opens a compact detail panel with its role and available real status.
- Support selection through an accessible parallel node list; canvas interaction cannot be the only option.
- Lazy-load 3D assets and stop unnecessary rendering when offscreen. Reduced-motion mode stops continuous decorative movement.
- WebGL failure must preserve all core functionality through a 2D diagram.
- Do not implement the two heroes as the same scene with only different headings.

## 3. Remove the Viva UI completely

- Remove the Viva navigation item, page, homepage cards, route links, and unused page-specific assets/components.
- Redirect old `/viva` links to the Lab homepage or provide an appropriate route migration.
- Do not rename the same page “Study guide” and leave it in the application.
- Keep CA1/CA2 explanations and report downloads where relevant. Existing standalone academic documentation can remain; a Viva page must not appear in either website.

## 4. Make Live a working detection console

Use `/console` as the main working surface. Its first viewport should contain run controls, current run state, a readable stream, and actual results.

Include:

- Start, pause/resume production, stop, and create a fresh run with explicit semantics.
- Scenario preset, seed, bounded event count/rate, active engine, and model version.
- Real lifecycle states: queued, starting, running, draining, completed, failed, cancelled.
- Separate counters for generated, broker-acknowledged, processed, persisted, flagged, failed, and late events, where instrumentation supports them.
- A transaction table with event time, account, amount, model score, rule result, final review decision, engine, and processing status.
- Search/filter by event/account/run, score range, status, scenario, and time.
- An alert queue with an evidence drawer, review decisions, and audit history.
- A selected transaction timeline from generation through durable result and UI receipt. Missing stages must remain explicitly missing.
- Streaming charts built from measured data; timestamps, time range, units, and freshness must be visible.
- JSON/CSV export of the selected run's permitted records.

Pausing generation must allow in-flight processing to drain; pausing the visual feed is a separate UI control. Resetting a view must not erase persistent evidence. Creating a new session must not affect other users.

### Ready-to-run scenario library

Provide presets with seeded input, short descriptions, bounded settings, expected conditions, and observed outcomes:

| Preset | Purpose | Evidence to display |
|---|---|---|
| Everyday traffic | Ordinary activity with a realistic mix of amounts | Predictions and observed false alerts |
| Transaction burst | Repeated activity from one synthetic account | Window/state features and rule evidence |
| Small tests, then a large payment | A sequence relevant to fraud review | Ordered account timeline and policy result |
| Spending deviation | A large change relative to available account history | Historical baseline, score, and rule reason |
| Delayed arrival | Controlled out-of-order timestamps | Watermark, late policy, and actual late records |
| Replay and duplicate delivery | Replayed stable event IDs | Attempts, unique outputs, deduplication evidence |

Only implement a scenario against model features that actually exist. Additional synthetic behavioural fields may support explicitly labelled rules, but must not be passed into a trained model that expects a different schema. Do not manufacture unavailable historical/device/location features.

Provide **Run preset**, **Inspect configuration**, and **Replay as new run**. A preset can navigate from a landing page but must not start costly or destructive work merely on page load.

Do not force an alert because a scenario is named suspicious. Send inputs through the actual model and policy, show the observed output, and investigate missed detections honestly. Demonstration fixtures are not unbiased model evaluation data.

## 5. Convert CA2 into an executable streaming workspace

CA2 must open on controls and evidence, not a wall of definitions. Use a working layout:

1. Top control strip: choose/join run, scenario, seed, rate, duration, and comparison configuration.
2. Parallel engine lanes: actual Flink and Spark status, job IDs, event counts, processing activity, and result timestamps.
3. Main chart/timeline area: measured delay distributions and matched-event results.
4. Compact panels for watermark/late events, checkpoints, backlog, and errors.
5. An expandable “How this works” area for theory.

Required actions:

- Start a bounded comparison from the Lab without visiting Live first.
- Join an existing permitted Live session without duplicating the generator.
- Follow one event through both engines and inspect matching identifiers.
- Inject controlled late events and show what actually happened.
- Inspect checkpoints and perform a protected recovery experiment on an isolated experiment job.
- Replay a saved configuration and export measurement evidence.

Use one input stream and independent consumption paths. Align feature, model, window, watermark, and output semantics before interpreting performance. Show resource allocation, versions, sample count, warm-up interval, run duration, and Spark trigger interval beside experiment details.

Report missing/duplicate/unmatched results, not only successful pairs. Distinguish provisional from finalized results. Define the delay measurement boundary and clock assumptions. Do not calculate an overall winner from incomparable or incomplete runs.

Support stateful scenarios only where both implementations have equivalent semantics, or explicitly mark the experiment as a functionality demonstration rather than a fair speed comparison.

## 6. Add a shared System Intelligence page

Expose **System Intelligence** in both applications, preferably at `/system`. Use one shared backend and consistent components. Preserve session/run filters when switching websites.

This is a practical engineering workspace, not an AI chat page or a dump of raw terminal output.

### A. Overview and topology

- Show Generator, API, Kafka, Flink, Spark, Scoring, Result Consumer, and Database, as implemented.
- Report healthy, degraded, unavailable, idle, or unknown based on fresh checks and metrics.
- Include last observation time and relevant evidence such as a job ID or successful result timestamp.
- Separate “process is reachable” from “transactions are successfully completing.” A running container alone is not an end-to-end health check.
- Display stale observations as stale. Never aggregate an unknown required dependency into a green “System OK.”

### B. Unified structured logs

Collect structured application and relevant engine lifecycle/error logs, with fields such as:

```text
timestamp_utc, observed_at, severity, service, environment,
session_id, run_id, event_id, trace_id, engine, job_id,
stage, status, duration_ms, message, error_code, retry_attempt
```

Use null/absent values when a field is unavailable; do not fabricate identifiers or durations. Track event time separately from log observation time.

Provide:

- Live tail with pause/resume display, reconnect, and an indication when the viewer is behind.
- Server-side filters for time range, severity, component, engine, run, event, and text.
- Pagination/virtualization, bounded retention, capped queries, and sanitized exports.
- A readable row summary and expandable structured payload/stack trace where permitted.
- Links from a log entry to its transaction, trace, alert, or experiment.
- A persisted history across page reloads; no reliance on browser-console logs as the data source.

Never expose API keys, cookies, tokens, full connection strings, credentials, or real personal/payment data. Enforce access checks on the backend. Public visitors see only sanitized logs for their permitted demo runs; operator logs and infrastructure detail require authorization.

### C. Transaction trace explorer

Searching an event ID should reveal:

`Generated → Kafka acknowledged → Engine received → Features ready → Model scored → Result persisted → Alert evaluated → Client received`

Show recorded spans/timestamps, retries, failures, and missing instrumentation. Link Flink and Spark traces for the same input. Label browser receipt timing separately from server processing delay. Use OpenTelemetry where appropriate or a documented equivalent correlation mechanism; avoid adding a heavyweight observability stack without need.

### D. Data quality and pipeline integrity

Include measured counts for malformed records, schema mismatches, missing required fields, duplicate delivery, processing failures, late records, unmatched engine outputs, and stale/unprocessed records where detectable.

Provide a quarantined/dead-letter record view with sanitized reasons and controlled reprocessing if implemented. Define the count semantics so retries do not inflate unique transactions. Reconcile input/output counts per run and explain why differences may remain while a run is draining.

### E. Model and stream operations

- Loaded model/version, artifact checksum or identifier, feature schema, threshold, inference errors, and measured inference timing.
- Flink job state, checkpoint age/status, restarts, and real watermark/backpressure metrics if exposed.
- Spark query state, latest batch ID/duration, progress, and configured trigger.
- Kafka consumer lag with the observed group/topic/partition scope.
- Error trends and active incidents derived from documented conditions.

Only display instrumentation that exists. When unsupported, state “not instrumented” rather than inserting zero. Keep risky controls operator-only, with a clear confirmation for a deliberate failure test. Public access must not become remote administration access.

## 7. Audit the LightGBM results honestly

The supplied CA1 screenshot shows LightGBM selected and displays approximately 15.3% precision, 29.6% recall, F1 0.201, and a held-out confusion matrix. Treat these as screenshot observations to verify against saved evaluation artifacts, not as proof of correct training or acceptable performance.

- Verify preprocessing consistency, categorical handling, label integrity, split strategy, drift, missing-value handling, and feature availability during live inference.
- Explain validation versus test metrics and confirm displayed results come from the correct artifact and model.
- Tune on training/validation only. Keep a final test set untouched by repeated tuning; if it has already guided tuning, disclose that and establish a new valid evaluation design where feasible.
- Investigate threshold tradeoffs and label imbalance without promising a target accuracy.
- If performance remains modest, report it accurately. Good presentation must not hide false positives and missed fraud.
- Distinguish LightGBM-only predictions from rules and the combined review policy everywhere in the UI and report.

## 8. Synchronization and implementation discipline

- Reuse the existing authentication/session design where suitable; inspect contracts before adding incompatible endpoints.
- Both websites consume the same durable backend state. Do not generate independent frontend statistics.
- Use correlation IDs and versioned event contracts across generator, Apache jobs, scoring, persistence, API, and frontend.
- Handle ordered reconnection, duplicate updates, and snapshot reconciliation. Render the last successful update time.
- Protect global controls and isolate visitor sessions. Rate-limit event generation, exports, AI requests, and experiment execution.
- Keep optional AI explanations separate from fraud scoring and log facts. Deterministic explanations must work without paid keys.
- Preserve model data and checkpoints during UI changes. Add migrations if persistence changes are required.

## 9. Cloud and budget requirements remain mandatory

- Real Kafka, Flink, and Spark processing must operate on GCP after deployment and feed both websites.
- Use the existing project `fraudwatch-508912`; reuse deployed services and URLs where possible.
- Require **₹0 out-of-pocket cost**. Verify non-billable Free Trial status, eligible credits, and expiry before creating chargeable resources. Do not upgrade billing or purchase anything.
- Explain trial-credit consumption separately from personal payment. Do not call temporary credit-funded hosting permanently free.
- If safe trial-funded deployment cannot be verified, finish independent code work, report the exact cloud blocker, and do not claim cloud completion.
- Do not claim Cloud Run frontends alone mean Apache has been deployed. Verify the actual backend jobs and their data path.
- Provide explicit cloud start/stop instructions and honest offline states. No laptop backend dependency after deployment.

## 10. Required verification

Run appropriate integration and browser checks, and retain evidence for this complete path:

1. Open both landing pages at desktop and mobile sizes. Inspect the real rendered 3D scene, typography, labels, buttons, fallback, and themes.
2. Confirm the Viva tab and page are removed and old links are handled.
3. Launch an ordinary preset in Live; observe real event IDs, scores, stored results, and logs.
4. Launch a suspicious preset; inspect actual model/rule evidence and the resulting review decision. If no alert occurs, diagnose and report it instead of forcing one.
5. Open the same run in Lab and verify synchronization.
6. Start a separate comparison from CA2 itself; verify actual Flink and Spark job activity, matched inputs, results, and charts.
7. Execute a late-event scenario and inspect its recorded outcome.
8. Search a transaction in System Intelligence and follow it across logs and traces.
9. Verify log redaction, access control, malformed-event handling, reconnect, and persisted history.
10. In an isolated operator experiment, test recovery and record observed checkpoint/output behaviour.
11. Stop local backend services and repeat a cloud smoke test through the deployed URLs.

Keep a feature/evidence checklist. “Rendered” and “connected to real data” are separate acceptance criteria. Do not mark a feature complete because its button or card exists.

## 11. Delivery

Implement in this order: inspect existing integrations; deliver the two landing pages and remove Viva; complete scenarios and the Live console; make CA2 executable; add System Intelligence; verify cloud operation and update evidence.

Return:

- The existing project's updated source and a concise change summary.
- Two verified URLs with direct links to Live Console, CA2, and System Intelligence.
- Desktop/mobile screenshots of both landing pages and the key working views.
- Test evidence, real run/job IDs, model/evaluation provenance, and any unresolved blockers.
- Updated setup instructions, environment examples, and the report/screenshots where affected.

Do not stop after designing the homepages. Finish the connected scenarios, live CA2 execution, and observability so that the visual experience demonstrates a real system.


---

# Part 3 — Final automatic cloud lifecycle

# FraudWatch — Start on Visit, Sleep When Idle

## Implement this in the existing project

Extend the current FraudWatch deployment plan with automatic wake-up and safe idle shutdown. This is an additional requirement for the existing two websites and real Apache backend, not a replacement project.

**Desired experience:** a person opens either website → the website loads → the cloud pipeline wakes if necessary → an actual bounded demo can run → after a long period without meaningful use, the backend saves its state and stops → the next visit starts it again without terminal commands or a laptop backend.

Preserve the previous requirements: real Kafka/Flink/Spark, LightGBM inference, synchronized Live and Lab, System Intelligence, no Viva UI, and ₹0 out-of-pocket cost using only verified eligible Free Trial resources. Do not upgrade billing. Do not claim cloud completion until this lifecycle has been tested on GCP.

## 1. Separate the two scaling mechanisms

### Cloud Run: websites, API/control service

- Use automatic scaling and minimum instances of zero at every applicable service/revision configuration. Set conservative maximum instances based on tested capacity.
- Prefer request-based billing for these request-driven components where appropriate.
- Incoming requests should activate the services normally. Allow Cloud Run to scale down when unused; do not claim its exact shutdown timing is controlled by our application idle timer.
- Keep the initial website and runtime-status endpoint available without a running Apache VM or database on that VM.
- Avoid constant synthetic traffic, unnecessary minimum instances, and keep-alive pings that defeat scale-down.
- Close unnecessary live connections and pause background polling when a session becomes inactive. A WebSocket or SSE connection is an ongoing request and cannot be ignored in the cost design.

### Compute Engine: persistent Apache stack

- Kafka, Flink, the database, and supporting stateful services run on the existing designated VM or the approved equivalent deployment. Spark runs when a comparison is requested.
- Cloud Run autoscaling does **not** automatically stop this VM. Implement explicit VM start and stop orchestration.
- Start and stop the same configured VM; do not create a new VM for every visitor and do not delete persistent data during idle shutdown.
- A cold wake can take time. Measure it and show progress rather than promising instant readiness.

## 2. Wake controller outside the sleeping VM

Implement a lightweight control service on Cloud Run, using a service account with narrowly scoped permissions for the designated VM and required control resources.

- Maintain runtime state, wake locks, activity leases, and operation IDs in durable storage accessible while the VM is stopped, such as a small Firestore control collection if appropriate and covered by the budget constraints.
- Do not store the only wake lock or activity record in PostgreSQL on the sleeping VM.
- Expose a validated session-level `ensure-ready` operation. Authenticate or issue a bounded anonymous demo session using the existing security model; public visitors must never receive GCP credentials or direct Compute API access.
- Use an idempotent, atomic wake lock so simultaneous visits join one start operation.
- Return promptly with a state and operation identifier. Continue orchestration using durable tasks/workflow steps, not an untracked in-memory thread after an HTTP response.
- Use a queue such as Cloud Tasks for short retryable state transitions and readiness checks if appropriate. Service-to-service callbacks must be authenticated and idempotent.
- A visitor arriving during shutdown must be recorded and served after a safe stop/restart transition; resolve races without losing the wake request.
- Apply startup deadlines, bounded retries, cooldowns, and clear failure states to avoid endless credit consumption on a broken deployment.
- Resolve backend connectivity correctly after a restart; do not rely on an ephemeral external IP remaining unchanged. Use appropriate private connectivity/service discovery and do not expose broker or database ports publicly.

## 3. Automatic behaviour on a real visit

- Opening either website in a visible browser session should automatically request readiness once, after loading the lightweight page and obtaining a valid demo session. No terminal command or manual VM start should be required.
- Do not wake the VM for static asset requests, health checks, link previews, repeated status polling, or monitoring probes. Apply anonymous-session limits and abuse controls; a browser visibility flag alone is not security.
- Once ready, the console can start one small seeded ordinary-traffic demo for that session, or attach to the session's existing run. Do not start duplicate generators on refresh or when the same session opens Lab.
- Make this automatic demo bounded: default to at most 100 transactions, within existing tested rate limits. It must execute actual processing and remain clearly labelled synthetic demo traffic.
- Larger scenarios, Spark benchmarks, and recovery experiments require their existing explicit controls and permissions. Merely opening a landing page must not launch a Spark benchmark or failure test.
- Show a clear state progression: `Sleeping → Starting VM → Starting services → Restoring processing → Ready`.
- Readiness requires database and broker checks, the expected active Flink job, the correct model, and a small successful end-to-end processing probe. VM `RUNNING` alone is insufficient.
- Preserve requested navigation and session/run identifiers across startup. On failure, show the observed failed stage and a bounded retry action.

## 4. Safe idle shutdown

Use **45 minutes** without meaningful activity as the initial configurable idle timeout. This policy applies to the Apache VM; it is separate from Cloud Run autoscaling.

Meaningful activity includes a recent real user interaction, bounded transaction generation, active experiment work, and processing that still needs to drain. It excludes status polling, metrics scraping, controller checks, decorative animation, and an abandoned browser tab.

- Track session leases and last interaction on the server. Active work has a bounded job lease and execution deadline, not an unlimited exemption.
- A read-only dashboard left open without interaction must eventually stop renewing activity. Explain this behaviour in a subtle session notice.
- Before shutdown, confirm all permitted sessions are idle, no valid job leases remain, and no wake operation is pending.
- Use durable delayed tasks to re-evaluate idle eligibility. Recheck current state atomically when a task executes; old timeout tasks must not stop a newly active system.
- Stop accepting new background generation, drain bounded in-flight work, persist results, and record a run disposition for unfinished work.
- Use an appropriate tested Flink checkpoint/savepoint and graceful cancellation/restoration procedure. Preserve Kafka data, consumer progress, Spark checkpoint state where relevant, and database records on persistent storage.
- Apply a documented maximum drain time and handle failures visibly. Do not discard unresolved records silently, leave the VM running indefinitely on failure, or falsely report successful recovery.
- Gracefully stop services and issue the VM stop operation. Confirm the stopped state in the controller and show **Sleeping to save trial credits**.
- Record lifecycle operations in System Intelligence. Store enough control history outside the VM to explain its sleeping/starting state.

Do not implement the idle timer only in the browser, only as `setTimeout` inside Cloud Run, or only in the VM that will be stopped. Browser closure and control-service restarts must not break the policy.

## 5. Configuration and cost controls

Add documented settings such as:

```dotenv
AUTO_WAKE_ENABLED=true
AUTO_DEMO_ENABLED=true
AUTO_DEMO_MAX_EVENTS=100
PIPELINE_IDLE_TIMEOUT_MINUTES=45
PIPELINE_VM_NAME=
PIPELINE_VM_ZONE=
PIPELINE_STARTUP_TIMEOUT_SECONDS=600
PIPELINE_DRAIN_TIMEOUT_SECONDS=180
PIPELINE_MIN_UPTIME_MINUTES=10
CONTROL_STATE_COLLECTION=fraudwatch-runtime
CONTROL_TASK_QUEUE=fraudwatch-lifecycle
```

These are starting configuration values, not measured guarantees. Validate and tune them against actual cloud startup, checkpoint, and drain behaviour. Add bounded job durations, wake-rate limits, and a total runtime/credit guard suitable for the verified remaining trial allowance. Budget alerts alone are not a hard spending cap.

Report costs separately:

- VM compute while running.
- Persistent disks/storage and other resources that remain allocated while sleeping.
- Cloud Run requests/compute, control-state reads/writes, tasks, images, builds, logs, and networking.

Stopping the VM reduces compute usage; it does not make all hosting free. Enforce the original no-paid-billing constraint and describe trial-credit consumption honestly. Do not deploy a costly dedicated controller VM or continually running polling worker to save the main VM's credits.

Provide a protected operator override to keep a demonstration active for a specified bounded duration. It must expire automatically and remain within the trial policy. Preserve public session isolation and limit who can alter lifecycle settings.

## 6. Verification required before claiming completion

1. Start with the Apache VM stopped. Open either deployed website and verify the page renders, requests readiness, and starts the actual cloud pipeline.
2. Verify a real transaction receives a LightGBM score, is persisted, and appears with the same run/event IDs in both sites.
3. Open both sites simultaneously from multiple sessions and verify only one VM start operation occurs.
4. Confirm refresh and cross-site navigation do not create duplicate demo runs for one session.
5. Trigger a Spark experiment after wake-up. Verify the ops-agent actually consumes the queued job and records success/failure; queued-only buttons do not count as implemented.
6. Exercise idle shutdown using a shortened test timeout, then restore the production setting to 45 minutes. Confirm the actual VM is stopped and no background connections/tasks keep it cycling.
7. Reopen the website after shutdown. Confirm automatic restart, previous evidence availability, successful new processing, and documented state recovery.
8. Test a visit during shutdown, an active experiment near the idle deadline, an abandoned tab, controller restarts, duplicate task delivery, a failed VM start, and a slow service restore.
9. Confirm logs contain lifecycle evidence without secrets and anonymous users cannot control arbitrary cloud resources.
10. Repeat the cloud lifecycle with all local backend services stopped. Record measured startup time, stop time, GCP operation IDs, job IDs, test results, and remaining limitations.

## 7. Finish the underlying implementation too

This deployment feature does not replace the unfinished work already reported. Complete the ops-agent/experiment launcher, real benchmarks, `.env.example`, setup/Apache/GCP documentation, and meaningful browser checks. Investigate the missing `tests/e2e` typecheck error and fix the intended test configuration rather than suppressing genuine errors.

Document this lifecycle in the README and System Intelligence page, including what wakes the backend, what counts as idle, what survives shutdown, and what still consumes credits. Preserve the existing deployments and URLs where possible.

## Official implementation references

- [Cloud Run automatic scaling and scaling to/from zero](https://docs.cloud.google.com/run/docs/about-instance-autoscaling)
- [Cloud Run minimum instance configuration](https://docs.cloud.google.com/run/docs/configuring/min-instances)
- [Compute Engine stop and restart behaviour](https://docs.cloud.google.com/compute/docs/instances/stop-start-instance)
- [Google Cloud Free Trial terms](https://docs.cloud.google.com/free/docs/free-cloud-features)

Implement and verify this behaviour; do not merely add an architecture paragraph or an unconnected start button.

