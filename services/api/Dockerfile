# Build context: repository root
FROM python:3.12.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /srv
COPY services/api/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY packages/fwcore /tmp/fwcore
COPY services/common /tmp/fwsvc
RUN pip install /tmp/fwcore /tmp/fwsvc && rm -rf /tmp/fwcore /tmp/fwsvc
COPY ml/results/stream_model_results.json ml/results/reference_model_results.json /srv/results/
COPY services/api/app /srv/app
RUN mkdir -p /srv/evidence && useradd --uid 10001 --create-home app
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 CMD curl -fsS http://localhost:8000/healthz || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--proxy-headers", "--forwarded-allow-ips", "*", "--no-access-log"]
