# FraudWatch — Master Build Prompt

## Your task

Build **FraudWatch: Real-Time Fraud Detection using Big Data Analytics and Machine Learning** as a complete, working academic project with **two synchronized websites in one repository and one GCP project**.

Deliver the implementation, real Apache integrations, trained ML model, tests, deployed URLs, source code, and a polished PDF case study. Do not stop at a plan, landing page, dashboard mockup, or simulated Apache integration. Work in milestones, verify each milestone, and accurately report anything that remains blocked.

Make the project visually impressive, technically defensible, and easy for four students to demonstrate and explain during a viva. Prioritize a coherent working system over a long list of superficial features.

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
- Use an economical academic-demo deployment and document estimated running costs, what stays on, and how to stop it. Obtain any required resource-spending authorization before incurring unapproved costs; continue local implementation while access or authorization is pending.

## Two websites, one shared system

### Website 1: FraudWatch Live

An operational fraud-monitoring demo. Use the deployment/service name `fraudwatch-live` where available.

The first screen should expose a useful dashboard and a clear **Start demo** action. Keep any introduction compact. A visitor must not scroll through a long marketing page to use the project.

Essential views:

1. **Live overview:** transaction stream, scored transactions, flagged count, observed processing delay, throughput, and actual service health.
2. **Transaction inspector:** transaction details, model score, rule evidence, contextual features, model version, and event timeline.
3. **Alert investigation:** filter alerts, inspect evidence, acknowledge an alert, and record a demo review decision with an audit trail. Review decisions must not silently retrain the model.
4. **Scenario controls:** start, pause, reset, choose a reproducible scenario, adjust supported event rate, and inject a suspicious pattern.
5. **Visual pipeline:** an interactive 3D architecture view with a usable 2D alternative.

Show INR amounts with appropriate formatting. Use synthetic account identifiers and sample merchants. Distinguish transaction amount flagged for review from confirmed fraud losses or money saved. This is an educational system, not a live banking integration or real payment-blocking service.

### Website 2: FraudWatch Lab

An interactive academic companion containing the complete CA1 and CA2 work. Use the deployment/service name `fraudwatch-lab` where available.

Organize it into concise sections or tabs:

- **CA1:** problem, dataset, preprocessing, model, results, and connection to the live demo.
- **CA2:** Flink implementation, event time, watermarks, state, checkpoints, recovery, and the Spark comparison.
- **Experiment lab:** start or inspect authorized benchmark runs, compare measured results, replay a run, and download evidence.
- **Architecture explorer:** the same system diagram, with deeper explanations on selection.
- **Viva mode:** guided demonstration, short concept cards, questions with expandable answers, and team details.
- **Report and evidence:** PDF download, methodology, model card, benchmark configuration, test summary, and references.

Make this an interactive study lab rather than a long report pasted onto a webpage. Use diagrams, compact tables, charts, and expandable details. Keep full prose in the downloadable report.

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

- Train a simple baseline such as logistic regression and one suitable tree-based model, then select using validation evidence.
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
7. **Viva walkthrough:** a guided five-to-seven-minute path through CA1, CA2, results, and limitations.

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
4. Finish the shared visual system, interactive diagrams, investigation tools, and viva walkthrough.
5. Deploy the two websites and required backend; verify synchronization and operator protection.
6. Produce the report from the final evidence, verify the PDF, and package the reproducible source.

Make routine implementation decisions independently. Ask only for missing information, access, or authorization that materially blocks the next step. Do not substitute planning for implementation or let optional AI polish delay the working CA1/CA2 system.

## Official reference starting points

Verify documentation against the exact versions selected during implementation:

- [Apache Flink Docker deployment](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/standalone/docker/)
- [Apache Spark Structured Streaming](https://spark.apache.org/streaming/)
- [Spark 3.5.6 streaming guide: micro-batch and continuous modes](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) — version-specific background, not a requirement to select this version.
- [Cloud Run WebSocket behaviour and connection considerations](https://docs.cloud.google.com/run/docs/triggering/websockets)

## Final handover

Return the two verified website URLs, PDF report, source archive/repository, exact run/deployment instructions, test and benchmark summary, current resource costs/stop procedure, and any remaining limitations. Make it straightforward for our group to operate, explain, and defend the actual work.
