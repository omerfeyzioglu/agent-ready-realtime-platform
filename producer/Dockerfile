# Stage 1: compile. CGO off gives a static binary that needs no C library.
FROM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /producer .

# Stage 2: run. Distroless static has CA certificates and time zone data
# (America/New_York) but no shell or package manager; runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /producer /producer
ENTRYPOINT ["/producer"]
