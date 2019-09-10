package com.examples.gcp.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;

public class BigQueryUtils {

    private BigQueryUtils() {

    }

    public static Schema getSchemaFromExistingTable(BigQuery bigQuery, String projectId, String datasetName, String tableName) {
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        return  bigQuery.getTable(tableId).getDefinition().getSchema();
    }
}
