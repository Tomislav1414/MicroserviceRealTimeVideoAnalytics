FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN sed -i 's|http://ports.ubuntu.com|https://ports.ubuntu.com|g' /etc/apt/sources.list; \
    opts="-o Acquire::Retries=3"; \
    ok=1; \
    for i in 1 2 3 4 5; do \
      if apt-get update $opts && apt-get install -y $opts --no-install-recommends \
        python3 \
        python3-pip \
        libglib2.0-0 \
        ca-certificates; then \
        ok=0; break; \
      fi; \
      echo "apt attempt $i failed, retrying in 5s..."; sleep 5; \
    done; \
    [ "$ok" -eq 0 ] || exit 1; \
    update-ca-certificates && rm -rf /var/lib/apt/lists/*

COPY detectorsdk /detectorsdk
RUN ok=1; \
    for i in 1 2 3 4 5; do \
      if pip3 install --no-cache-dir -U 'setuptools>=68' wheel && \
         pip3 install --no-cache-dir --no-build-isolation /detectorsdk; then ok=0; break; fi; \
      echo "pip install detectorsdk failed (attempt $i), retrying in 5s..."; sleep 5; \
    done; \
    [ "$ok" -eq 0 ] || exit 1

COPY humandetector/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --no-cache-dir --force-reinstall opencv-python-headless

COPY humandetector/ .

CMD ["python3", "main.py"]
