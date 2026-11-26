FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
# Allow a private proxy CA for module downloads without changing runtime trust.
RUN --mount=type=secret,id=go_download_ca \
    --mount=type=tmpfs,target=/tmp/go-download \
    set -eu; \
    if [ -f /run/secrets/go_download_ca ]; then \
        cat /etc/ssl/certs/ca-certificates.crt > /tmp/go-download/ca-certificates.crt; \
        printf '\n' >> /tmp/go-download/ca-certificates.crt; \
        cat /run/secrets/go_download_ca >> /tmp/go-download/ca-certificates.crt; \
        export SSL_CERT_FILE=/tmp/go-download/ca-certificates.crt; \
    fi; \
    go mod download

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

COPY cmd ./cmd
COPY internal ./internal
RUN --network=none CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X github.com/dkoshenkov/chaos-go/internal/server.version=$VERSION -X github.com/dkoshenkov/chaos-go/internal/server.commit=$COMMIT -X github.com/dkoshenkov/chaos-go/internal/server.buildDate=$BUILD_DATE" \
    -o /out/chaos-proxy ./cmd/chaos-proxy
RUN --network=none CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -buildvcs=false \
    -ldflags="-s -w" -o /out/chaos-tunnel ./cmd/chaos-tunnel

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/chaos-proxy /chaos-proxy
COPY --from=build /out/chaos-tunnel /chaos-tunnel

USER 65532:65532
ENV SERVER_LISTEN=:8080
EXPOSE 8080
ENTRYPOINT ["/chaos-proxy"]
