FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /sessionapi .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /sessionapi /sessionapi
EXPOSE 8090
ENTRYPOINT ["/sessionapi"]
