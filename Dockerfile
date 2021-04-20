FROM node:14.15-alpine as base

RUN apk update
RUN apk add --no-cache curl tzdata && \
    cp /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime && \
    echo "America/Sao_Paulo" > /etc/timezone && \
    apk del tzdata

ENV NODE_ENV ${NODE_ENV}

USER root 
WORKDIR /app
ADD package.json /app
ADD package-lock.json /app
RUN npm install
ADD . /app

RUN ["chmod","+x","/app/start_worker.sh"]

ENTRYPOINT ["sh","-c","/app/start_worker.sh"]
