package demo.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;

/**
 * @author chenj
 * @date 2019-02-04 22:42
 * @email 924943578@qq.com
 */
public class BigQuerySnippets {

    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();


    /** Example of listing datasets, specifying the page size. */
    // [TARGET listDatasets(DatasetListOption...)]
    public static Page<Dataset> listDatasets() {
        // [START bigquery_list_datasets]
        // List datasets in the default project
        Page<Dataset> datasets = bigquery.listDatasets(BigQuery.DatasetListOption.pageSize(100));
        System.out.println("datasets:");
        for (Dataset dataset : datasets.iterateAll()) {
            // do something with the dataset
            System.out.println(dataset.getDatasetId().getDataset());
        }
        // [END bigquery_list_datasets]
        return datasets;
    }

    /** Example of listing the tables in a dataset, specifying the page size. */
    // [TARGET listTables(String, TableListOption...)]
    // [VARIABLE "my_dataset_name"]
    public static Page<Table> listTables(String datasetName) {
        // [START ]
        System.out.println("tables:");
        Page<Table> tables = bigquery.listTables(datasetName, BigQuery.TableListOption.pageSize(100));
        for (Table table : tables.iterateAll()) {
            // do something with the table
            System.out.println(table.getTableId().getTable());
        }
        // [END ]
        return tables;
    }


    /** Example of loading a newline-delimited-json file with textual fields from GCS to a table. */
    //sourceUri gs://staging.cobalt-particle-229608.appspot.com/demo/userSessionsData.json
    // [TARGET create(JobInfo, JobOption...)]
    // [VARIABLE "my_dataset_name"]
    // [VARIABLE "my_table_name"]
    public static Long writeRemoteFileToTable(String datasetName, String tableName, String sourceUri, FormatOptions options)
            throws InterruptedException {
        // [START bigquery_load_table_gcs_json]
//        String sourceUri = "gs://staging.cobalt-particle-229608.appspot.com/demo/userSessionsData.json";
        TableId tableId = TableId.of(datasetName, tableName);
        // Table field definition
        LoadJobConfiguration configuration =
                LoadJobConfiguration.builder(tableId, sourceUri)
                        .setFormatOptions(options)
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        .setAutodetect(true)
                        .build();
        // Load the table
        Job loadJob = bigquery.create(JobInfo.of(configuration));
        loadJob = loadJob.waitFor();
        // Check the table
        System.out.println("State: " + loadJob.getStatus().getState());
        return ((StandardTableDefinition) bigquery.getTable(tableId).getDefinition()).getNumRows();
        // [END bigquery_load_table_gcs_json]
    }


    /** Example of listing table rows, specifying the page size. */
    // [TARGET listTableData(TableId, TableDataListOption...)]
    // [VARIABLE "my_dataset_name"]
    // [VARIABLE "my_table_name"]
    public static TableResult listTableData(String datasetName, String tableName) {
        // [START bigquery_browse_table]
        TableId tableIdObject = TableId.of(datasetName, tableName);
        // This example reads the result 100 rows per RPC call. If there's no need to limit the number,
        // simply omit the option.
        TableResult tableData =
                bigquery.listTableData(tableIdObject, BigQuery.TableDataListOption.pageSize(100));
        System.out.println("rows:");
        for (FieldValueList row : tableData.iterateAll()) {
            // do something with the row
            for(int i = 0;i < row.size();i++)
             System.out.print(row.get(i).getStringValue() + " ");
            System.out.println();
        }
        // [END bigquery_browse_table]
        return tableData;
    }

    /** Example of running a Legacy SQL query. */
    public static void runLegacySqlQuery() throws InterruptedException {
        // [START bigquery_query_legacy]
        // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        String query = "select * from babynames.sessiondata;";
        QueryJobConfiguration queryConfig =
                // To use legacy SQL syntax, set useLegacySql to true.
                QueryJobConfiguration.newBuilder(query).setUseLegacySql(true).build();

        // Print the results.
        for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
            for (FieldValue val : row) {
                System.out.printf("%s,", val.getStringValue());
            }
            System.out.printf("\n");
        }
        // [END bigquery_query_legacy]
    }

    /** Example of creating a dataset. */
    // [TARGET create(DatasetInfo, DatasetOption...)]
    // [VARIABLE "my_dataset_name"]
    public static Dataset createDataset(String datasetName) {
        // [START bigquery_create_dataset]
        Dataset dataset = null;
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        try {
            // the dataset was created
            dataset = bigquery.create(datasetInfo);
        } catch (BigQueryException e) {
            // the dataset was not created
            e.printStackTrace();
        }
        // [END bigquery_create_dataset]
        return dataset;
    }

}
