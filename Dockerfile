# Multi-stage: compila estático en golang y corre en una imagen mínima.
# El host NO necesita Go: todo se compila dentro del contenedor.

# ---- build ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache de dependencias (go.mod + go.sum primero).
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/onix-ingestor ./cmd/onix-ingestor

# ---- runtime ----
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/onix-ingestor /onix-ingestor
EXPOSE 8081
USER nonroot:nonroot
# Healthcheck sin shell/curl: el propio binario hace ping a /healthz.
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/onix-ingestor", "-healthcheck"]
ENTRYPOINT ["/onix-ingestor"]
