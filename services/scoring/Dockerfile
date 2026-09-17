# Build context: repository root
FROM python:3.12.14-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl && rm -rf /var/lib/apt/lists/*
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv
COPY services/scoring/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY packages/fwcore /tmp/fwcore
RUN pip install /tmp/fwcore && rm -rf /tmp/fwcore
COPY ml/artifacts/fw-ieee-stream-1.0.0 /models/fw-ieee-stream-1.0.0
COPY services/scoring/app /srv/app
RUN useradd --uid 10001 --no-create-home app
USER app
ENV MODEL_ARTIFACT_PATH=/models/fw-ieee-stream-1.0.0 SCORING_WORKERS=2
EXPOSE 8001
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 CMD curl -fsS http://127.0.0.1:8001/healthz || exit 1
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8001 --workers ${SCORING_WORKERS} --no-access-log"]
