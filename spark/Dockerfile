FROM spark:4.1.1-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
      -o /opt/spark/jars/postgresql-42.7.5.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.8/clickhouse-jdbc-0.9.8-all.jar \
      -o /opt/spark/jars/clickhouse-jdbc-0.9.8-all.jar

WORKDIR /opt/project
