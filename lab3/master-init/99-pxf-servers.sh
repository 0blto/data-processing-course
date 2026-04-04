_lab_pxf_servers() {
  set -e
  if [[ ! -d /run/lab-pxf-servers/pg_crm ]]; then
    echo "INFO - /run/lab-pxf-servers/pg_crm отсутствует, пропуск установки PXF-сервера"
    return 0
  fi

  source /home/gpadmin/.bashrc
  source /usr/local/greenplum-db/greenplum_path.sh

  mkdir -p /data/pxf/servers
  rm -rf /data/pxf/servers/pg_crm
  cp -a /run/lab-pxf-servers/pg_crm /data/pxf/servers/

  pxf cluster sync
  echo "INFO - Скопирован pg_crm в /data/pxf/servers и выполнен pxf cluster sync"
}

_lab_pxf_servers
