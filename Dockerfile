FROM alpine:3.6
ADD _furrow /bin/furrow
USER root
ENTRYPOINT ["/bin/furrow"]
