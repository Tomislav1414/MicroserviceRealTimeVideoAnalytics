
FROM postgres:16-alpine
COPY init.sh /init.sh
COPY retention.sh /retention.sh
COPY schema.sql /schema.sql
COPY pipeline.sql /pipeline.sql
COPY shared.sql /shared.sql
RUN chmod +x /init.sh /retention.sh
ENTRYPOINT ["/init.sh"]
