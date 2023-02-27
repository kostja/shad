FROM postgres
ENV POSTGRES_HOST_AUTH_METHOD trust
ENV POSTGRES_DB graph
ENV PGDATA /var/lib/postgres/data/graph/

COPY graph.csv /var/lib/postgresql/data/graph/
COPY sql/init.sql /docker-entrypoint-initdb.d/
