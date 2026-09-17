# Build context: repository root
FROM python:3.12.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv
COPY services/result-consumer/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY services/common /tmp/fwsvc
RUN pip install /tmp/fwsvc && rm -rf /tmp/fwsvc
COPY services/result-consumer/app /srv/app
RUN useradd --uid 10001 --create-home app
USER app
CMD ["python", "-m", "app.main"]
