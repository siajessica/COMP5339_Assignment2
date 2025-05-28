FROM debian:bullseye

# Environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV SPARK_CONNECT_MODE=legacy

# Install system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        python3 \
        python3-pip \
        wget \
        curl \
        ca-certificates \
        gnupg \
        lsb-release && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# Default shell
CMD ["/bin/bash"]