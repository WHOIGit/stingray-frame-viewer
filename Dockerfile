# ---------- builder ----------
FROM python:3.13-slim AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Build into a self-contained venv so the runtime stage can copy a single
# directory and forget about pip / Python install paths.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# ---------- runtime ----------
FROM python:3.13-slim

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Drop privileges. UID 1000 is conventional; override at runtime
# (`docker run --user`) if the host volume's ownership demands a different one.
RUN useradd --create-home --shell /bin/bash --uid 1000 stingray
USER stingray

EXPOSE 8000

# Bind to 0.0.0.0 inside the container; the compose port mapping decides
# what the host exposes (default: 127.0.0.1 only).
CMD ["uvicorn", "stingray_frame_viewer.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000"]
