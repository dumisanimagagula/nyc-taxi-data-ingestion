#!/usr/bin/env bash
set -euo pipefail

export HIVE_CONF_DIR="${HIVE_CONF_DIR:-/opt/hive/conf}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx2g -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://metastore-db:5432/metastore -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=hive"

echo "Checking Hive Metastore schema..."
if /opt/hive/bin/schematool -dbType postgres -info >/tmp/schema_info.log 2>&1; then
  echo "Schema exists. Skipping initialization."
else
  echo "Schema missing. Initializing..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi

echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore --verbose
