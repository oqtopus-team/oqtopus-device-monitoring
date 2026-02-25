FROM alpine:3.20
RUN apk add --no-cache \
    bash \
    curl \
    coreutils \
    tzdata

WORKDIR /app
COPY relabel_weekly.sh /usr/local/bin/relabel_weekly.sh
RUN chmod +x /usr/local/bin/relabel_weekly.sh

RUN echo "1 0 * * * /usr/local/bin/relabel_weekly.sh >> /var/log/cron.log 2>&1" > /etc/crontabs/root
CMD ["crond", "-f", "-l", "2"]
