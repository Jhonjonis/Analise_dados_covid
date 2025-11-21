FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/output
RUN mkdir -p /opt/spark/app
RUN mkdir -p /opt/spark/data

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# copiar script de entrada
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

WORKDIR /opt/spark/app

ENTRYPOINT ["/opt/entrypoint.sh"]
