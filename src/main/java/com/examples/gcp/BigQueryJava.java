package com.examples.gcp;

import com.examples.gcp.utils.BigQueryUtils;
import com.google.cloud.bigquery.*;

import java.io.IOException;


import java.util.UUID;

public class BigQueryJava {

    private static final String PROJECT_ID = "bigquery-examples-252504";
    private static final String DATASET_NAME = "test_dataset";
    private static final String TABLE_NAME = "movies";
    private static final String MOVIES_28K_URI = "gs://alpha-beta/movies_ndjson_28k.json";



    public static void main(String[] args) throws InterruptedException {
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        // create dataset
        Dataset dataset = createDatasetOrGet(bigQuery);

        // create table
        // NB. this step can be skipped if you mention schema directly in the load job configuration and set create disposition
        Table table = createTableOrGet(bigQuery);

        // load data using job
        // this method is useful when loading data from a file, all at once
        Long numRecordsCreated = loadJob(bigQuery, table);

        // SKIPPED - load data using insertAll
        // this method is useful when you are consuming data from some other source
        // you can stream them in batches


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
            table = bigQuery.create(movieTableInfo);
            System.out.printf("Table [%s] created. \n", TABLE_NAME);
        }
        return table;
    }

    private static Schema createMovieSchema() {

        // NB. could also use GSON or other Json parser to create the fields and schema directly from the json dataset file

        Field title = Field.newBuilder("title", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
        Field year = Field.newBuilder("year", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build();
        Field cast = Field.newBuilder("cast", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        Field genres = Field.newBuilder("genres", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        return Schema.of(title, year, cast, genres);
    }

    private static Long loadJob(BigQuery bigQuery, Table table) throws InterruptedException {
        // format of job, in case you plan to use REST: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
        // check out JobConfiguration: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration

        // for load job, create a JobConfigurationLoad
        // format: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        // class ref for Cloud library, for API library it's different: https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/LoadJobConfiguration.html
        LoadJobConfiguration loadJobConfiguration = LoadJobConfiguration.builder(table.getTableId(), MOVIES_28K_URI)
                .setFormatOptions(FormatOptions.json())
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .setSchema(table.getDefinition().getSchema())
                .build();


        Job loadJob = bigQuery.create(JobInfo.of(loadJobConfiguration));    // same as bigQuery.jobs().insert(job) if you are using Google API libraries
                                                                            // or same as 'jobs/insert' endpoint, if you are using REST
                                                                            // this project uses Cloud library so it's bigQuery.create(job)
        // wait for it to finish
        loadJob = loadJob.waitFor();


        System.out.printf("load job status: [%s] \n", loadJob.getStatus().getState());
        StandardTableDefinition updatedTable = bigQuery.getTable(table.getTableId()).getDefinition();
        return updatedTable.getNumRows();
    }

    private static void queryJob(BigQuery bigQuery, Table table, String queryString) {

    }
}