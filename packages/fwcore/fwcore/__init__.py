"""FraudWatch shared core.

Everything that must behave identically across the generator, training code,
scoring service, Flink job (re-implemented in Java) and Spark job lives here,
so there is a single reference implementation to test the engines against.
"""

SCHEMA_VERSION = "1.1"
FEATURE_SPEC_VERSION = "fs-1.1"
RULESET_VERSION = "rules-1.1"
POLICY_VERSION = "policy-1.0"

# Amounts are the IEEE-CIS ``TransactionAmt`` values exactly as provided in the source files.
# No currency conversion is applied anywhere. The dataset files do not state a unit, so the
# unit is documented as unspecified ("source unit").
AMOUNT_UNIT = "source unit (IEEE-CIS TransactionAmt, unit unspecified in the data files; no conversion)"
DATASET = "IEEE-CIS Fraud Detection, provided by Vesta Corporation through the Kaggle competition ieee-fraud-detection"
