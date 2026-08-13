# Uses the postgres image only for its psql client — the pipeline is registered
# over RisingWave's Postgres wire protocol (port 4566).
FROM postgres:16-alpine
COPY init.sh /init.sh
COPY schema.sql /schema.sql
COPY pipeline.sql /pipeline.sql
COPY shared.sql /shared.sql
RUN chmod +x /init.sh
ENTRYPOINT ["/init.sh"]
