# Build context: repository root
FROM python:3.12.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /srv
COPY services/generator/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY packages/fwcore /tmp/fwcore
COPY services/common /tmp/fwsvc
RUN pip install /tmp/fwcore /tmp/fwsvc && rm -rf /tmp/fwcore /tmp/fwsvc
# IEEE-CIS replay pack (built by ml/training/build_replay_pack.py; row-level, not in git). Labels are NOT copied.
COPY ml/artifacts/replay/ieee-test-replay-1.0/records.json ml/artifacts/replay/ieee-test-replay-1.0/selections.json ml/artifacts/replay/ieee-test-replay-1.0/manifest.json /models/replay/ieee-test-replay-1.0/
COPY services/generator/app /srv/app
RUN useradd --uid 10001 --create-home app
USER app
EXPOSE 8002
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 CMD curl -fsS http://localhost:8002/healthz || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "1"]
