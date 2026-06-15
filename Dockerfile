FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="ServiceHC"
LABEL org.opencontainers.image.description="Health checks for Kafka, Kafka Connect, Java consumers, Flink, and Flink jobs"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/servicehc

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        openjdk-17-jre-headless \
        procps \
        systemd \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/ ./scripts/
COPY config/ ./config/
COPY README.md ./

RUN chmod +x ./scripts/healthcheck.py ./scripts/run-healthcheck.sh

ENTRYPOINT ["python3", "/opt/servicehc/scripts/healthcheck.py"]
CMD ["--help"]
