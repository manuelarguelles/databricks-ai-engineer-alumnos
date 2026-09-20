#!/usr/bin/env bash
set -euo pipefail

profile="${1:-${DATABRICKS_CONFIG_PROFILE:-}}"
warehouse_id="${DATABRICKS_WAREHOUSE_ID:-}"

if [[ -z "$profile" ]]; then
  echo "Uso: $0 <perfil>" >&2
  echo "Opcional: DATABRICKS_WAREHOUSE_ID=<id> $0 <perfil>" >&2
  exit 2
fi

if ! command -v databricks >/dev/null 2>&1; then
  echo "ERROR: no se encontró el Databricks CLI en PATH." >&2
  exit 3
fi

profiles_json="$(databricks auth profiles -o json 2>&1)" || {
  echo "ERROR: no se pudieron consultar los perfiles del Databricks CLI." >&2
  exit 4
}

profile_info="$(printf '%s' "$profiles_json" | PROFILE_NAME="$profile" python3 -c '
import json, os, sys
payload = json.load(sys.stdin)
profiles = payload.get("profiles", payload) if isinstance(payload, dict) else payload
wanted = os.environ["PROFILE_NAME"]
match = next((p for p in profiles if p.get("name") == wanted), None)
if match is None:
    sys.exit(1)
print(match.get("host", ""))
print("YES" if match.get("valid") else "NO")
')" || {
  echo "ERROR: el perfil '$profile' no existe. Usa databricks auth login." >&2
  exit 5
}

host="$(printf '%s\n' "$profile_info" | sed -n '1p')"
valid="$(printf '%s\n' "$profile_info" | sed -n '2p')"
if [[ "$valid" != "YES" ]]; then
  echo "ERROR: el perfil '$profile' no es válido. Renueva el login OAuth." >&2
  echo "Perfil: $profile"
  echo "Host: $host"
  exit 6
fi

if ! databricks current-user me -p "$profile" -o json >/dev/null 2>&1; then
  echo "ERROR: autenticación fallida para '$profile'. Renueva el login o revisa permisos." >&2
  exit 7
fi

echo "OK autenticación"
echo "Perfil: $profile"
echo "Host: $host"

warehouses_json="$(databricks warehouses list -p "$profile" -o json 2>/dev/null)" || {
  echo "ERROR: autenticado, pero no fue posible listar warehouses (permisos o red)." >&2
  exit 8
}
echo "OK warehouses: listado de solo lectura"

if [[ -n "$warehouse_id" ]]; then
  if printf '%s' "$warehouses_json" | WAREHOUSE_ID="$warehouse_id" python3 -c '
import json, os, sys
payload = json.load(sys.stdin)
items = payload.get("warehouses", payload) if isinstance(payload, dict) else payload
sys.exit(0 if any(str(item.get("id", "")) == os.environ["WAREHOUSE_ID"] for item in items) else 1)
'; then
    echo "OK warehouse asignado: visible"
  else
    echo "ERROR: el warehouse asignado no es visible para este perfil." >&2
    exit 9
  fi
fi

echo "Conexión verificada sin iniciar compute ni exponer credenciales."
