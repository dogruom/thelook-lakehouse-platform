FROM alpine:3.19

RUN apk add --no-cache curl libc6-compat \
    && curl -fSL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x /usr/local/bin/mc

CMD ["mc"]
