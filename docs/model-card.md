# Model card — `fw-ieee-stream-1.1.0`

Every number on this page was copied from two committed artifacts:
`ml/artifacts/fw-ieee-stream-1.1.0/metadata.json` and
`ml/artifacts/fw-ieee-stream-1.1.0/evaluation.json`, both produced by
`ml/training/train_stream_model.py`. Nothing here is estimated or rounded from memory.

| | |
|---|---|
| Model version | `fw-ieee-stream-1.1.0` |
| Model type | LightGBM gradient-boosted trees (`lightgbm` 4.7.0) |
| Feature spec | `fs-1.1` |
| Ruleset / policy version | `rules-1.1` / `policy-1.0` |
| Seed | 20260917 |
| Trained at | unix 1789665017 (`metadata.json` → `trained_at_unix`) |
| Training environment | Python 3.12.12, Windows-11-10.0.22631-SP0, scikit-learn 1.9.1, LightGBM 4.7.0, pandas 3.0.5, numpy 2.5.3 |
| Reproduce | `cd ml && uv run python training/ieee_data.py && uv run python training/train_stream_model.py` |

## 1. Task

Binary scoring of a single card transaction, in a stream, from that transaction plus the
**stream-derived history of its pseudo-account**. The output is one score in `[0, 1]` per event, plus a
policy decision, plus per-prediction TreeSHAP contributions.

This is a *streaming demonstration* model. It is not a production fraud-decisioning system, and it was
not built to beat a Kaggle leaderboard (it deliberately uses 15 stream-computable features instead of
the ~430 columns the dataset offers).

## 2. Score semantics — read this before quoting any number

`metadata.json` states verbatim:

> `"score_semantics": "model score in [0,1]; not a calibrated probability unless calibration results support it"`

So: **the score is a ranking score, not a probability of fraud.** Calibration *was* measured on the
held-out test split (Brier 0.03210647336603684, ECE 0.005508451998131233, with 10 reliability bins
stored in `evaluation.json`), and on unseen accounts (Brier 0.02665601522479358, ECE
0.004537916577964159). Those numbers are good, but a low ECE at a 3.5% base rate is easy to achieve by
predicting small numbers everywhere, so treat the score as a rank and quote the calibration numbers as
calibration numbers — not as "the model says there is an X% chance of fraud".

Explanations are `LightGBM pred_contrib=True` (TreeSHAP), in **log-odds** units, with the bias term
equal to the expected value (`-3.864479198688447` on the sampled test rows). They explain the model,
not the world, and they are not causal.

## 3. Dataset

IEEE-CIS Fraud Detection, provided by Vesta Corporation through the Kaggle competition
[`ieee-fraud-detection`](https://www.kaggle.com/competitions/ieee-fraud-detection). The raw CSVs are
**not** redistributed in this repository; you must accept the competition rules and download them
yourself.

| | |
|---|---|
| Files | `train_transaction.csv` (683,351,067 bytes, sha256 `3a5c83ab…3d642`), `train_identity.csv` (26,529,680 bytes, sha256 `b63c725d…3c37c`) |
| Join | `train_transaction LEFT JOIN train_identity ON TransactionID` (1:0..1, row count unchanged) |
| Rows | 590,540 |
| Label | `isFraud` from `train_transaction.csv` |
| Overall fraud rate | 0.03499000914417313 |
| Rows with an identity record | 144,233 |
| Pseudo-account (`uid`) | `card1 + addr1 + P_emaildomain` — the dataset has **no** account id |

### Amounts have no currency

`metadata.json` records `amount_unit` as:

> `"source unit (IEEE-CIS TransactionAmt, unit unspecified in the data files; no conversion)"`

The dataset does not state a currency for `TransactionAmt`. This project therefore shows amounts as
bare numbers with **no currency symbol and no conversion of any kind** (in particular, no INR
conversion). The rule thresholds in section 5 are in those same unconverted source units. Any UI or
document that attaches a currency to these numbers is wrong.

### Split

Chronological by `TransactionDT`, ties broken by `TransactionID`, **no shuffling**, fractions
0.70 / 0.15 / 0.15:

| Split | Rows | Fraud | Legit | Fraud rate |
|---|---|---|---|---|
| train | 413,378 | 14,538 | 398,840 | 0.03516878014795175 |
| validation | 88,581 | 3,042 | 85,539 | 0.03434145019812375 |
| test | 88,581 | 3,083 | 85,498 | 0.03480430340592226 |

Split boundaries in event-time seconds: validation starts at 10,438,003, test starts at 13,151,880.

A time-ordered split is the honest choice for a fraud stream — a random split would let the model see
the future of an account it is scoring. Note the consequence: the test split's PR-AUC is *lower* than
the validation split's (0.1375 vs 0.1831), which is exactly the temporal drift a shuffled split would
have hidden.

`metadata.json` also records the leakage rule for state features:

> "State features use each account's earlier transactions in any split as history only; labels are never
> used as features."

### Feature parity check

`metadata.json` → `dataset.parity_check`: 405 uids, 34,801 events, `max_relative_diff`
3.668121910221923e-13 between the offline (pandas) feature build and the streaming implementation. The
Flink image additionally runs `FeatureParityTest` at build time against `tests/streaming/fixtures`, so
the Java features cannot silently drift from `fwcore`.

## 4. Features (`fs-1.1`)

Nine numeric, six categorical. All of them are computable from the current event plus the account's
running state — nothing requires a batch join at scoring time.

**Numeric:** `log_amount`, `prior_count_60s`, `prior_count_10m`, `prior_count_1h`,
`log_prior_amount_sum_1h`, `log_secs_since_last`, `log_prior_count_total`, `log_amount_ratio`,
`has_history`.

**Categorical (with fixed category lists baked into the artifact):**

| Feature | Categories |
|---|---|
| `product_cd` | W, C, R, H, S |
| `card_network` | visa, mastercard, american express, discover, unknown |
| `card_type` | debit, credit, other, unknown |
| `email_domain` | gmail.com, yahoo.com, hotmail.com, anonymous.com, aol.com, other, missing |
| `device_type` | desktop, mobile, unknown |
| `has_identity` | no, yes |

The category lists are closed. Anything unseen maps to the explicit `unknown` / `other` / `missing`
member, which is why the model cannot fail on a new email domain — and also why it cannot learn
anything about one.

Top features by LightGBM total split gain (`evaluation.json` → `global_importance.gain`, full list of
15 in the artifact):

| Feature | Gain |
|---|---|
| `log_amount` | 89936.73045039177 |
| `product_cd` | 89354.82442975044 |
| `device_type` | 35841.22425699234 |
| `email_domain` | 33155.511476278305 |
| `log_secs_since_last` | 32627.331864356995 |
| `log_amount_ratio` | 26090.43726849556 |

SHAP global importance is also stored, computed as mean `|TreeSHAP contribution|` over a seeded random
sample of 20,000 held-out test rows.

## 5. Rules and policy

The rules (`packages/fwcore/fwcore/rules.py`, `rules-1.1`) are **evidence, not model attributions**, and
are deliberately independent of how the label was created. Their thresholds are in unconverted source
amount units:

| Rule | Condition | Fires on the test split |
|---|---|---|
| `R1_BURST` | ≥ 4 earlier transactions from the same account in the previous 60 s of event time | 625 |
| `R2_PROFILE_DEVIATION` | ≥ 5 earlier transactions, and this amount is ≥ 8x the running mean and ≥ 60 | 497 |
| `R3_TEST_THEN_LARGE` | ≥ 3 earlier transactions under 2.5 within 10 min, then this one ≥ 25 | **0** |

`R3_TEST_THEN_LARGE` never fires on the held-out split. That is a real finding, reported as-is: the
card-testing pattern the rule describes does not occur in this data at these thresholds. It is not
evidence that the rule is broken, and it should not be presented as a working detector.

### Thresholds

| Threshold | Value | How it was chosen |
|---|---|---|
| alert | 0.10477242918040724 | max F1 on the **validation** split |
| review | 0.06954678512105691 | `min(alert, top 10% of validation scores)` |

Both were selected on validation and then applied unchanged to test — `evaluation.json` records
`thresholds.selected_on = "validation"`. A 62-point `validation_threshold_sweep` is stored in the
artifact if you want to see the trade-off curve rather than the single point.

Policy outcome on the held-out test split (`evaluation.json` → `test_policy`):

| Bucket | Count | Share | Fraud rate within |
|---|---|---|---|
| alert | 7,515 | 0.08483760625867849 | 0.1507651363938789 |
| review | 3,901 | 0.044038789356634044 | 0.08792617277621123 |
| allow | 77,165 | 0.8711236043846875 | 0.02082550379057863 |

Combined recall of alert + review (`flagged_recall`): **0.4787544599416153**. So roughly half of the
fraud in the test split lands in a bucket a human would look at, and the other half is allowed through.

## 6. Results

### Held-out test split (n = 88,581)

| Metric | LightGBM (selected) | Logistic regression (baseline) |
|---|---|---|
| PR-AUC | **0.1375002907626466** | 0.0960610558891451 |
| ROC-AUC | **0.7817200781725175** | 0.7603072482164691 |
| Precision @ threshold | 0.1507651363938789 | 0.1420464946000366 |
| Recall @ threshold | 0.3674991891015245 | 0.25170288679857283 |
| F1 @ threshold | 0.2138139271560672 | 0.181605429440674 |
| Accuracy @ threshold | 0.905939196893239 | 0.9210440162111514 |
| Flagged rate @ threshold | 0.08483760625867849 | 0.06167236766349443 |
| Threshold used | 0.10477242918040724 | 0.7926848398689657 |
| Validation PR-AUC | 0.1830727491251426 | 0.1438909056578056 |

Confusion matrix for LightGBM at the alert threshold: tn 79,116 / fp 6,382 / fn 1,950 / tp 1,133.
At the review threshold: tn 76,026 / fp 9,472 / fn 1,628 / tp 1,455, giving precision
0.13315640157408254 and recall 0.47194291274732403.

**Accuracy is a trap here.** Predicting "never fraud" on the test split scores about 96.5% accuracy and
catches nothing. Read PR-AUC and the precision/recall pair; the accuracy column is printed only so the
comparison is complete, and note that the *worse* model has the *higher* accuracy.

The baseline was selected fairly: `train_stream_model.py` fits both a `class_weight="balanced"`
logistic regression and LightGBM, and picks whichever has the higher **validation** PR-AUC. LightGBM
won (0.1831 vs 0.1439) and became the artifact.

### Cold-start slice: accounts never seen in training

`evaluation.json` → `test_unseen_accounts`:

| | |
|---|---|
| n | 10,961 |
| Positives / prevalence | 324 / 0.029559346774929295 |
| PR-AUC | 0.16883248466790515 |
| ROC-AUC | 0.7814471266729108 |
| Precision / recall / F1 @ alert | 0.19373219373219372 / 0.20987654320987653 / 0.20148148148148148 |
| Flagged rate | 0.03202262567284007 |
| Confusion matrix | tn 10,354 / fp 283 / fn 256 / tp 68 |
| "All legitimate" baseline accuracy | 0.9704406532250707 |

Ranking quality holds up on accounts with no history (ROC-AUC 0.7814 vs 0.7817 overall), but **recall
at the fixed threshold drops to 0.21**, roughly half the overall figure. That is the expected cost of a
model whose strongest signals after `log_amount` are behavioural. The threshold is not re-tuned for
this slice.

### Hyperparameters

`LGBMClassifier(n_estimators=3000, learning_rate=0.03, num_leaves=63, min_child_samples=100,
subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0, random_state=20260917)`, with
`early_stopping(150)` on validation `average_precision`. No class reweighting for LightGBM — the
imbalance is handled by tuning the decision threshold instead; the logistic-regression candidate uses
`class_weight="balanced"` on the training split only. The selected `best_iteration` is computed by the
training script but is **not** stored in either committed artifact, so the effective tree count is
_not recorded_ here.

## 7. Intended use

- Demonstrating a streaming fraud-scoring pipeline end to end, with the same model behind both Flink
  and Spark.
- Comparing two stream engines on identical inputs, model and features.
- Teaching: chronological splitting, PR-AUC over accuracy on imbalanced data, threshold selection,
  per-prediction TreeSHAP, cold-start degradation.

## 8. Out of scope

- Any real payment, authorisation, chargeback, onboarding or credit decision.
- Any decision about a real person. The pseudo-accounts are `card1 + addr1 + P_emaildomain` tuples from
  an anonymised research dataset, not people.
- Any claim about a currency, a market, or a monetary amount — the unit is unknown (section 3).
- Transfer to another dataset or another payment population. Nothing here was validated off
  IEEE-CIS.

## 9. Known limitations

1. **The score is not a probability** (section 2).
2. **Half the fraud is missed at the operating point.** `flagged_recall` 0.4788, and alert-bucket
   precision 0.151 means roughly 6 false alerts per true one.
3. **Cold-start recall is about half the overall recall** (0.2099 vs 0.3675).
4. **Temporal drift is real and visible**: validation PR-AUC 0.1831 → test PR-AUC 0.1375. Performance
   on data newer than the dataset is unknown.
5. **`R3_TEST_THEN_LARGE` never fires** on the held-out split.
6. **Pseudo-accounts are a heuristic.** `card1 + addr1 + P_emaildomain` can merge two real accounts or
   split one. Every "prior count" feature inherits that error.
7. **Missing values are informative here** (65,706 missing `addr1`, 94,456 missing `P_emaildomain`,
   only 144,233 of 590,540 rows have identity data) and the model can exploit missingness patterns that
   may be artefacts of how the dataset was assembled.
8. **Label provenance is unexplained.** The competition describes `isFraud` as a reported
   chargeback-related label; how it was produced, and what fraud it omits, is not documented in the
   data.
9. **Single seed, single training run.** No cross-validation, no confidence intervals, no repeated
   seeds — so small differences between the two candidate models should not be over-read.
10. **Fairness was not evaluated.** No slice metrics by any demographic or geographic attribute exist,
    and the anonymised features would not support a credible analysis anyway. Treat this as
    _not measured_, not as evidence of fairness.
11. **No drift monitoring or scheduled retraining** exists in this repository.

## 10. Not measured

Stated explicitly so nobody fills these in by assumption:

- **A published reference model for this exact 15-feature stream setup** — _not measured yet_. There is
  no committed comparison against a full-column Kaggle-style model, so no "we are within X of
  state of the art" claim can be made.
- **Fairness / subgroup metrics** — _not measured_ (see limitation 10).
- **Scoring-service latency and throughput under load** — _not measured yet_; see
  [`benchmark-methodology.md`](benchmark-methodology.md). The artifact contains no timing figures.
- **Confidence intervals on every metric above** — _not measured_ (single run, single seed).
- **Performance on any dataset other than IEEE-CIS** — _not measured_.
