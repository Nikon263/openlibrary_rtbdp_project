# for Python-related Dockerfile instructions, see
# https://gitlab.inf.unibz.it/rtbdp/resources/docker/-/tree/2023-2024/compose-example-3/server-python

FROM python:3.10.13-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install -U pip && \
    pip install -r requirements.txt
COPY . .
CMD [ "python3", "producer.py" ]
