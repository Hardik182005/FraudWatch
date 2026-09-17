# FraudWatch — Dataset Requirement Override

Apply this instruction to the existing FraudWatch project and to every earlier master, UI, visualization, deployment, and report prompt. **Where an earlier prompt permits a different dataset or a synthetic fallback, this instruction takes precedence.** Preserve the other requirements.

## Required dataset

Use **IEEE-CIS Fraud Detection**, the Kaggle competition dataset **`ieee-fraud-detection`**, provided by **Vesta Corporation**.

We have already downloaded this dataset. Locate and inspect the existing files and their schema before making changes. Reuse them; do not download a replacement, switch to another fraud dataset, or generate a substitute training dataset. If the files cannot be located, request their path and continue independent work rather than silently substituting data.

## Training and evaluation

- Train the requested **LightGBM** model and logistic-regression baseline using the available labelled IEEE-CIS records.
- Verify the actual label field, file relationships, join keys, missing values, and row counts. Avoid joins that duplicate transactions or inadvertently drop records without documentation.
- Use documented train/validation/held-out partitions appropriate to the available time information. Fit preprocessing on training data only, tune using validation data, and preserve the final held-out evaluation.
- Never use the fraud label, identifiers that leak outcomes, or future information as model inputs.
- Do not assume every competition file contains ground-truth labels. Calculate classification metrics only from records with verified labels.
- Version the dataset manifest, split definitions, preprocessing, feature order/types, model artifact, threshold, and evaluation outputs. Report the actual number of rows used and disclose any sampling.
- If a model is already trained, verify its provenance and serving compatibility. Do not retrain or replace it solely to change a displayed dataset name.

## Live demo and CA2

Use **replayed IEEE-CIS transaction records** as the normal input for the live demo, Kafka, Flink, Spark, and LightGBM scoring.

- This is live processing of historical records, not a connection to a live bank feed. Label the interface accordingly.
- Use the same feature schema and preprocessing as training. Keep evaluation labels outside the prediction request and attach them only in the evaluation path when appropriate.
- Keep replay arrival time separate from source event-time information. Document any mapping used to demonstrate event-time processing.
- Replay the same selected records to Flink and Spark for comparisons, with stable source references and distinct run identifiers.
- For a labelled demonstration, predefined fraud/ordinary selections may use verified source labels, but do not force the model to predict those labels. Show actual predictions, including false positives and missed fraud.
- Label such selected demonstrations as selected examples. They are not representative performance estimates.

## Modified scenarios: explicit boundary

Keep the requested late-arrival, duplicate-delivery, and replay demonstrations by modifying delivery timing or transport behaviour around dataset records. Preserve the original record and transformation metadata.

If a scenario changes model feature values, label it **“Modified IEEE-CIS demo scenario”**, keep it separate from evaluation, and do not assume the original fraud label remains valid after modification. Do not create a second synthetic training dataset.

Before implementing account-burst, spending-history, location, device, merchant, or other behavioural scenarios, verify that the required information and semantics actually exist in the downloaded data. Do not present anonymized fields as identified customers, real merchants, bank accounts, geographic locations, or verified histories without evidence. Mark application-created session/group identifiers as demo metadata, not source facts.

If an earlier requested feature cannot be supported by the dataset, adapt it to a defensible dataset-based demonstration or mark it unsupported. Do not invent source attributes to satisfy a UI card.

Do not relabel amounts as INR or apply a currency conversion unless the dataset documentation supports that interpretation and the transformation is explicit. Otherwise display “Transaction amount” with the source unit described as documented or unspecified.

## Visualizations, logs, and report

- Confusion matrices, precision/recall/F1, PR/ROC curves, class balance, calibration, and model explanations must come from verified IEEE-CIS-derived artifacts and the corresponding model/split.
- Keep original held-out evaluation, selected-record replay results, and modified scenarios clearly separate.
- Predictions without verified labels cannot be used to invent a confusion matrix. A review decision or rule output is not automatically ground truth.
- Show dataset provenance, model version, run/split reference, and transformation status wherever needed to interpret results.
- Document the dataset as **“IEEE-CIS Fraud Detection, provided by Vesta Corporation through the Kaggle competition ieee-fraud-detection.”** Verify applicable usage and redistribution terms before publishing row-level data or including it in public downloads.
- Keep raw dataset files and sensitive artifacts out of public repositories and frontend bundles. Use permitted, bounded, sanitized examples for public demonstrations.

## Acceptance

Confirm that training, scoring, both websites, Flink/Spark experiments, figures, exports, and the report use this dataset consistently. Remove obsolete references to alternative or synthetic training datasets. Verify the data and model paths rather than merely changing labels in the interface.

This override does not authorize cloud spending, paid APIs, billing upgrades, a new project, or removal of any working feature that remains compatible with these requirements.
