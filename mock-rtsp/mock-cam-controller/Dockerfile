FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o controller .

# ── Final image ───────────────────────────────────
FROM alpine:3.19

WORKDIR /app
COPY --from=builder /app/controller .

EXPOSE 3000
ENTRYPOINT ["./controller"]