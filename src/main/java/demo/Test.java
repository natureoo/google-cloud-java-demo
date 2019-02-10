package demo;

import demo.storage.StorageSnippets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author chenj
 * @date 2019-02-04 17:15
 * @email 924943578@qq.com
 */
public class Test {



    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        String datasetName = "test_mpp_dataset";
        String tableName = "test_mpp_table";

//        BigQuerySnippets.createDataset(datasetName);

//        BigQuerySnippets.listDatasets();


//        BigQuerySnippets.listTables(datasetName);

//        BigQuerySnippets.listTableData(datasetName, tableName);

//        BigQuerySnippets.runLegacySqlQuery();

//        String sourceUri = "gs://test-mpp-bucket/demo/test-mpp-blob";
//
//        BigQuerySnippets.writeRemoteFileToTable(datasetName, tableName, sourceUri, FormatOptions.csv());


        String bucketName = "test-mpp-bucket";
        String blobName = "demo/test-mpp-blob";
//        StorageSnippets.createBucket(bucketName);

//        StorageSnippets.listBucketIamMembers(bucketName);

       
//        createBlob(bucketName, blobName);

    }

    private static void createBlob(String bucketName, String blobName) throws FileNotFoundException {
        File file = new File("FL_insurance_sample.csv");
        FileInputStream inputStream = new FileInputStream(file);
        StorageSnippets.createBlobFromInputStream(bucketName, blobName, inputStream, "text/csv");

    }



}
