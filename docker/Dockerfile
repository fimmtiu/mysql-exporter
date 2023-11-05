FROM golang:1.19.3-bullseye AS base

ENV APP_HOME /mysql-exporter
WORKDIR $APP_HOME

RUN apt purge --autoremove --yes curl git

COPY . .

RUN go build
WORKDIR $APP_HOME/panic-monitor
RUN go install

ENV BUGSNAG_NOTIFY_RELEASE_STAGES production,staging

CMD ["/go/bin/panic-monitor", "/mysql-exporter/mysql-exporter"]
