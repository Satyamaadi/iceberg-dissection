FROM apache/spark:3.5.1

USER root

# Install Python + tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Ensure pip is updated
RUN pip3 install --upgrade pip

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# Set working directory
WORKDIR /app
COPY . /app

# Spark + Iceberg config (important)
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENV SPARK_MODE=local

# Default run command
CMD ["bash", "-c", "spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 /app/app.py && sleep infinity"]