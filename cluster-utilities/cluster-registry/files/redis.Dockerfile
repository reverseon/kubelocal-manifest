FROM ubuntu:22.04

ENV REDIS_VERSION=7.0.15

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download and compile Redis
RUN cd /tmp && \
    wget http://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz && \
    tar xzf redis-${REDIS_VERSION}.tar.gz && \
    cd redis-${REDIS_VERSION} && \
    make -j$(nproc) && \
    make install && \
    cd / && \
    rm -rf /tmp/redis-${REDIS_VERSION}*

# Create directories
RUN mkdir -p /etc/redis && \
    useradd -r -u 1000 -g users chloe && \
    chown -R chloe:users /etc/redis

USER chloe
WORKDIR /etc/redis
EXPOSE 6379

CMD ["echo", "please override with required redis configuration"]