FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
ARG TARGETOS TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/retcd .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/retcd /retcd
ENV LISTEN_ADDR=":2379" \
    REDIS_ADDR="localhost:6379"
EXPOSE 2379
USER nonroot:nonroot
ENTRYPOINT ["/retcd"]
