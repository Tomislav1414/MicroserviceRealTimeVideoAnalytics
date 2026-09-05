FROM golang:1.25-alpine AS build
RUN apk add --no-cache build-base pkgconfig ffmpeg-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /decoder .

FROM alpine:3.24
RUN apk add --no-cache ffmpeg ca-certificates
COPY --from=build /decoder /decoder
ENTRYPOINT ["/decoder"]
