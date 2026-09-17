"""FraudWatch shared core.

Everything that must behave identically across the generator, training code,
scoring service, Flink job (re-implemented in Java) and Spark job lives here,
so there is a single reference implementation to test the engines against.
"""

SCHEMA_VERSION = "1.0"
FEATURE_SPEC_VERSION = "fs-1.0"
RULESET_VERSION = "rules-1.0"
POLICY_VERSION = "policy-1.0"

# The IEEE-CIS training data is denominated in USD. The whole pipeline uses INR
# as its canonical currency, so training amounts are converted once with this
# fixed, documented reference rate. It is an illustrative constant, not a live
# exchange rate, and it is identical in training and in every engine.
REFERENCE_INR_PER_USD = 88.0
