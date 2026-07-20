FROM alpine:3.19

RUN apk add --no-cache ffmpeg netcat-openbsd bash

WORKDIR /app
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENV RETRY_DELAY=2

ENTRYPOINT ["./entrypoint.sh"]
