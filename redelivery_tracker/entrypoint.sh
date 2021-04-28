#!/usr/bin/dumb-init /bin/sh

set -e

exec /usr/bin/redelivery_tracker "$@"
