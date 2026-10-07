FROM alpine:3.19

RUN apk add --no-cache curl libc6-compat \
    && curl -fSL -o /usr/local/bin/minio https://dl.min.io/server/minio/release/linux-amd64/minio \
    && chmod +x /usr/local/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]

CMD ["minio", "server", "/data", "--console-address", ":9001"]
