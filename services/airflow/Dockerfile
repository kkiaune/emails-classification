FROM puckel/docker-airflow

ENV HTTP_PROXY=http://docker.for.win.localhost:3128
ENV HTTPS_PROXY=http://docker.for.win.localhost:3128

USER root

RUN pip install --no-cache-dir \
    minio \
    numpy==1.15.4 \
    pandas==0.24.2 \
    scikit-learn==0.20.3 \
    scipy==1.2.1
