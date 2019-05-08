FROM jupyter/scipy-notebook

ENV HTTP_PROXY=http://docker.for.win.localhost:3128
ENV HTTPS_PROXY=http://docker.for.win.localhost:3128

USER root

RUN pip install --no-cache-dir \
    html2text \
    psycopg2-binary \
    minio
