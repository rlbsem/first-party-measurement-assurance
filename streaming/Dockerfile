FROM apache/beam_python3.12_sdk:2.77.0@sha256:359e2b669741f12a9b557793e9d47cf755e69f3ef714dc2831154d12bd27a662
COPY requirements-streaming.lock /opt/assurance/requirements-streaming.lock
RUN python -m pip install --no-cache-dir -r /opt/assurance/requirements-streaming.lock
COPY streaming /opt/assurance/streaming
ENV PYTHONPATH=/opt/assurance
# Preserve the SDK harness entrypoint. Cloud Run cleanup explicitly overrides the command.
