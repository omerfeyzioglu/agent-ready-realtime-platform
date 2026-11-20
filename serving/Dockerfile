# Ops API image (FastAPI + MCP). Build from the serving/ folder with the
# feature definitions passed in as a second build context:
#   docker buildx build --platform linux/amd64 --provenance=false \
#     --build-context features=../features -t <repo>:<tag> --push .
FROM python:3.13-slim

# Runs as an unprivileged user; the cluster also enforces a read-only root
# filesystem, so everything the app needs is baked in here.
RUN useradd --uid 10001 --create-home api
WORKDIR /srv/serving

# Dependencies first (cached layer): the pinned list from pyproject.toml.
COPY pyproject.toml .
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" \
      > /tmp/requirements.txt \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

USER api
# DuckDB downloads its extensions on first use; do it at build time so the
# running pod needs no write access and no download. iceberg loads avro
# (its manifest format) on its own, so avro must be here too.
RUN python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'aws', 'iceberg', 'avro')]"

COPY --chown=api app/ app/
COPY --from=features --chown=api live.yaml /srv/features/live.yaml

# config.py finds features/live.yaml under REPO_ROOT.
ENV REPO_ROOT=/srv PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
