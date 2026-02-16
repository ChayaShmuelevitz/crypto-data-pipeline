FROM python:3.11-slim

WORKDIR /dbt

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && pip install --no-cache-dir dbt-athena-community awscli \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY . /dbt

CMD ["bash"]