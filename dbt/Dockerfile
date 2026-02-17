FROM python:3.11-slim

WORKDIR /dbt

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && pip install --no-cache-dir dbt-athena-community awscli \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.dbt

COPY profiles.yml /root/.dbt/profiles.yml

COPY . /dbt

CMD ["bash"]