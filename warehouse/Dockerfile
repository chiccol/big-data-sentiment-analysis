FROM postgres:17.0

ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=password
ENV POSTGRES_DB=warehouse

COPY initdb.sql /docker-entrypoint-initdb.d/

RUN chmod 644 /docker-entrypoint-initdb.d/initdb.sql
