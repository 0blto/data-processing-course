FROM apache/spark:3.5.4-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/spark/work-dir

COPY streaming_job.py /opt/spark/work-dir/streaming_job.py
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh \
    && chmod +x /entrypoint.sh \
    && chown spark:spark /opt/spark/work-dir/streaming_job.py \
    && mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

ENTRYPOINT ["/entrypoint.sh"]
