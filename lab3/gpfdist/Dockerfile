FROM woblerr/greenplum:6.27.1

USER root
RUN mkdir -p /load && chown gpadmin:gpadmin /load

USER gpadmin
WORKDIR /load

EXPOSE 8080

ENTRYPOINT []
CMD ["/bin/bash", "-lc", "source /usr/local/greenplum-db/greenplum_path.sh && exec gpfdist -d /load -p 8080 -V"]
