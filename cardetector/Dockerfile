FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

WORKDIR /app

# A transparent HTTP proxy on this network path serves corrupted .deb files from
# ports.ubuntu.com (arm64), causing "Hash Sum mismatch" on every attempt - retries
# over plain HTTP just hit the same corrupting proxy again. Switching the mirror to
# HTTPS routes around it (a MITM proxy can't silently rewrite TLS payloads without
# failing certificate validation). Retries stay as a defense-in-depth for genuine
# transient network blips.
# The `break`-on-success/`sleep`-on-failure pattern here must end each iteration on a
# command whose exit status reflects the real outcome - if the loop merely fell off the
# end after 5 failures, RUN would see the last `sleep`'s exit 0 and report success on a
# layer that never actually installed anything. `ok`/`exit 1` make exhaustion fatal.
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

# detectorsdk is a local sibling package (build context is ./backend), not on PyPI.
# python3-pip on Ubuntu 22.04 drags in apt's python3-setuptools (59.6.0, pre-PEP-621)
# onto /usr/lib/python3/dist-packages. Even with build-system.requires = ["setuptools
# >=68"] in pyproject.toml, pip's isolated build env for the *this* package still ends
# up shadowed by that apt copy and silently produces a package named "UNKNOWN" instead
# of "detectorsdk" (reproduced: apt setuptools present -> UNKNOWN; removed -> correct
# name). Upgrading setuptools/wheel into system site-packages first and installing
# with --no-build-isolation sidesteps the isolated env entirely. Retry: PyPI fetches
# here have hit transient TLS hiccups on this network.
COPY detectorsdk /detectorsdk
RUN ok=1; \
    for i in 1 2 3 4 5; do \
      if pip3 install --no-cache-dir -U 'setuptools>=68' wheel && \
         pip3 install --no-cache-dir --no-build-isolation /detectorsdk; then ok=0; break; fi; \
      echo "pip install detectorsdk failed (attempt $i), retrying in 5s..."; sleep 5; \
    done; \
    [ "$ok" -eq 0 ] || exit 1

COPY cardetector/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --no-cache-dir --force-reinstall opencv-python-headless

COPY cardetector/ .

CMD ["python3", "main.py"]
