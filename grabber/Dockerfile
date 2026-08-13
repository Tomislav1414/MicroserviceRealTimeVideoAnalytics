FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /grabber .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=build /grabber /grabber
ENTRYPOINT ["/grabber"]
