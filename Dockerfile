# Data Pipeline Dockerfile
# Extends candidate-submission:latest base image
# Optimized for 2 vCPUs and 2GB RAM constraint

FROM candidate-submission:latest

# Set working directory
WORKDIR /app

# Install system dependencies (if not present in base image)
# Note: All dependencies should be installable without internet access
# assuming they are cached or available in the base image
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
# Using --no-cache-dir to reduce memory footprint
# Using --no-index and --find-links for offline installation if needed
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY pipeline/ /app/pipeline/
COPY config/ /app/config/

# Ensure pipeline package is importable
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Set Spark configuration for 2 vCPUs and 2GB RAM
ENV SPARK_WORKER_CORES=2
ENV SPARK_WORKER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=1g
ENV SPARK_DRIVER_MEMORY=1g

# Create data directories
RUN mkdir -p /data/input /data/output/bronze /data/output/silver /data/output/gold

# Set proper permissions
RUN chmod -R 755 /app /data

# Health check to verify pipeline components are ready
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pipeline.utils; import pipeline.ingest; import pipeline.transform; import pipeline.provision; import pipeline.validate; import pipeline.data_quality; print('All modules loaded successfully')" || exit 1

# Default command runs the full pipeline in sequence, then validates
# Can be overridden for specific layer execution
CMD ["sh", "-c", "python3 -m pipeline.ingest && python3 -m pipeline.transform && python3 -m pipeline.provision && python3 -m pipeline.validate"]

# Labels for documentation
LABEL maintainer="Data Engineering Team" \
      description="Data Pipeline with Bronze-Silver-Gold architecture" \
      version="1.0.0" \
      resource.constraints="2vCPU,2GB RAM" \
      optimizations="Broadcast Hash Joins, Partition Pruning, Reduced Shuffle Partitions"
