# big-query-examples
big query java examples

### Note: I have already uploaded datasets to storage bucket

## To build and run: 
`export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"`

`mvn clean package`

`mvn exec:java -Dexec.mainClass=com.examples.gcp.BigQueryJava`

## To load data:
`mvn exec:java -Dexec.mainClass=com.examples.gcp.BigQueryJava -Dexec.args="loadData [URL_OF_THE_FILE]"`

## To run a query:
`mvn exec:java -Dexec.mainClass=com.examples.gcp.BigQueryJava -Dexec.args="runQuery 'SELECT title FROM [bigquery-examples-252504.test_dataset.movies] LIMIT 10'"`
