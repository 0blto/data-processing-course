$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
docker compose exec -u gpadmin master bash -lc "source /usr/local/greenplum-db/greenplum_path.sh && if [ -d /run/lab-pxf-servers/pg_crm ]; then mkdir -p /data/pxf/servers && rm -rf /data/pxf/servers/pg_crm && cp -a /run/lab-pxf-servers/pg_crm /data/pxf/servers/; fi && pxf cluster sync"
