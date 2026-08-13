FROM golang:1.25-alpine AS build
RUN apk add --no-cache build-base pkgconfig ffmpeg-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /decoder .

# Must match the build stage's Alpine version exactly — ffmpeg's shared
# library sonames (e.g. libavcodec.so.62) change across Alpine releases,
# and a mismatch fails at runtime with "symbol not found", not at build time.
FROM alpine:3.24
RUN apk add --no-cache ffmpeg ca-certificates
COPY --from=build /decoder /decoder
ENTRYPOINT ["/decoder"]
