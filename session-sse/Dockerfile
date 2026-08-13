FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /session-sse .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /session-sse /session-sse
EXPOSE 8095
ENTRYPOINT ["/session-sse"]
