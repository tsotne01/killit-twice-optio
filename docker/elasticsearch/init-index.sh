#!/usr/bin/env bash
set -euo pipefail

ES_HOST="${ELASTICSEARCH_NODE:-http://localhost:9200}"

echo "Waiting for Elasticsearch at ${ES_HOST} to be ready..."
until curl -s "${ES_HOST}/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
  sleep 2
done

echo "Checking if 'customers_v1' index exists..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${ES_HOST}/customers_v1")

if [ "$HTTP_CODE" -eq 200 ]; then
  echo "Index 'customers_v1' already exists."
else
  echo "Creating 'customers_v1' index with strict mappings..."
  curl -s -X PUT "${ES_HOST}/customers_v1" -H 'Content-Type: application/json' -d'{
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "refresh_interval": "1s"
    },
    "mappings": {
      "properties": {
        "id": { "type": "long" },
        "external_id": { "type": "keyword" },
        "email": { "type": "keyword" },
        "first_name": { "type": "text" },
        "last_name": { "type": "text" },
        "status": { "type": "keyword" },
        "version": { "type": "integer" },
        "balance": { "type": "double" },
        "attributes": { "type": "object" },
        "created_at": { "type": "date" },
        "updated_at": { "type": "date" }
      }
    }
  }'
  echo ""
  
  echo "Creating alias 'customers' -> 'customers_v1'..."
  curl -s -X POST "${ES_HOST}/_aliases" -H 'Content-Type: application/json' -d'{
    "actions": [
      { "add": { "index": "customers_v1", "alias": "customers" } }
    ]
  }'
  echo ""
fi

echo "Elasticsearch schema configuration completed."
