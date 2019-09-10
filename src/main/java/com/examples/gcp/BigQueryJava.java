package com.examples.gcp;

import com.google.cloud.bigquery.*;

import java.io.IOException;


import java.util.UUID;

public class BigQueryJava {

    private static final String PROJECT_ID = "bigquery-examples-252504";
    private static final String DATASET_NAME = "test_dataset";
    private static final String TABLE_NAME = "movies";


    public static void main(String[] args) throws IOException, InterruptedException {
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        Dataset dataset = createDatasetOrGet(bigQuery);
        Table movieTable = createTableOrGet(bigQuery);

    }

    private static Dataset createDatasetOrGet(BigQuery bigQuery) {
        DatasetId datasetId = DatasetId.of(PROJECT_ID, DATASET_NAME);
        Dataset dataset = bigQuery.getDataset(datasetId);

        if (dataset == null) {
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(DATASET_NAME).build();
            dataset = bigQuery.create(datasetInfo);

            System.out.printf("Dataset [%s] created! \n", DATASET_NAME);
        }
        return dataset;
    }

    private static Table createTableOrGet(BigQuery bigQuery) {
        TableId tableId = TableId.of(PROJECT_ID, DATASET_NAME, TABLE_NAME);
        Table table = bigQuery.getTable(tableId);

        if (table == null) {
            Schema movieSchema = createMovieSchema();
            TableDefinition movieTableDef = StandardTableDefinition.of(movieSchema);
            TableInfo movieTableInfo = TableInfo.newBuilder(tableId, movieTableDef).build();
            Table movieTable = bigQuery.create(movieTableInfo);
            System.out.printf("Table [%s] created. \n", TABLE_NAME);
        }
        return table;
    }

    private static Schema createMovieSchema() {
        Field title = Field.newBuilder("title", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
        Field year = Field.newBuilder("year", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build();
        Field cast = Field.newBuilder("cast", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        Field genres = Field.newBuilder("genres", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        return Schema.of(title, year, cast, genres);
    }
}