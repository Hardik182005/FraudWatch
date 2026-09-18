# Build context: repository root
FROM python:3.12.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /srv
COPY services/ops-agent/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY services/common /tmp/fwsvc
RUN pip install /tmp/fwsvc && rm -rf /tmp/fwsvc
# Evaluation-only labels for the replayed IEEE-CIS records (table replay_labels). Never served to a
# client, never sent to scoring: they exist so the Lab's offline audit has ground truth.
COPY ml/artifacts/replay/ieee-test-replay-1.0/labels.json ml/artifacts/replay/ieee-test-replay-1.0/manifest.json /models/replay/ieee-test-replay-1.0/
COPY services/ops-agent/app /srv/app
# The container talks to the Docker Engine API over the mounted socket, so it runs as the group that
# owns that socket on the host. The uid is unprivileged; DOCKER_GID is set per host in compose.
RUN useradd --uid 10001 --create-home app
USER app
EXPOSE 8003
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 CMD curl -fsS http://localhost:8003/healthz || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003", "--workers", "1"]
