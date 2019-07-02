#!/bin/bash
PIDFILE="/stor/go/src/github.com/Conjur0/go-backend/go-backend.pid"
LOGFILE="/stor/go/src/github.com/Conjur0/go-backend/go-backend.`date +'%Y-%m-%d'`.log"
ERRFILE="/stor/go/src/github.com/Conjur0/go-backend/go-backend.err"

if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= | grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then
  echo "Already running."
  exit 99
fi

/stor/go/src/github.com/Conjur0/go-backend/go-backend 1>>$LOGFILE 2>>$ERRFILE &

echo $! > "${PIDFILE}"
chmod 644 "${PIDFILE}"

