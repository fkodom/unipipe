ARG PY_VERSION=3.9

FROM python:$PY_VERSION-slim
ENV DEBIAN_FRONTEND="noninteractive"

WORKDIR /app
COPY ./ ./unipipe
RUN pip install ./unipipe
