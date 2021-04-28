FROM docker-hub.repo.splunkdev.net/alpine:3.11
LABEL maintainer "#scp-lsdc <LSDCDev@splunk.com>"

ARG UID=998
ARG GID=998
RUN apk update \
 && apk add --no-cache ca-certificates \
 && wget -O /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 \
 && chmod +x /usr/bin/dumb-init \
 && rm -rf /var/cache/apk/* \
 && rm -rf /tmp/* /var/tmp/* \
 && addgroup -S -g $GID splunkuser \
 && adduser -D -S -H -u $UID -G splunkuser splunkuser
USER $UID:$GID
COPY --chown=splunkuser:splunkuser ./redelivery_tracker/entrypoint.sh build/linux/redelivery_tracker /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
