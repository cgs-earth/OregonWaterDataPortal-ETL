FROM alpine:latest

COPY ./monitor.sh /monitor.sh

# Ensure the script is executable
RUN chmod +x /monitor.sh

# Install required packages
RUN apk add --no-cache \
    docker-cli \
    curl \
    grep

CMD ["/bin/sh", "/monitor.sh"]
