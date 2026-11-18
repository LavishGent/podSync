# Builds examples/ratelimit. Usage: docker build -t podsync-ratelimit:dev .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /ratelimit ./examples/ratelimit

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ratelimit /ratelimit
EXPOSE 8080 9090
ENTRYPOINT ["/ratelimit"]
