FROM symfoni/furrow:latest
RUN apk update \
    && apk add --no-cache docker
RUN addgroup root docker

ADD start.sh /start.sh
RUN chmod a+x start.sh

ENTRYPOINT ["./start.sh"]
