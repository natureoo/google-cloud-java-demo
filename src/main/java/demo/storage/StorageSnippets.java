package demo.storage;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.*;
import com.google.cloud.storage.testing.RemoteStorageHelper;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * @author chenj
 * @date 2019-02-05 17:07
 * @email 924943578@qq.com
 */
public class StorageSnippets {

    private static final Storage storage;

    static{
        RemoteStorageHelper helper = RemoteStorageHelper.create();
        storage = helper.getOptions().getService();
    }


    /** Example of creating a bucket. */
    // [TARGET create(BucketInfo, BucketTargetOption...)]
    // [VARIABLE "my_unique_bucket"]
    public static Bucket createBucket(String bucketName) {
        // [START createBucket]
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
        // [END createBucket]
        return bucket;
    }

    /** Example of listing the Bucket-Level IAM Roles and Members */
    public static Policy listBucketIamMembers(String bucketName) {
        // [START view_bucket_iam_members]
        // Initialize a Cloud Storage client
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // Get IAM Policy for a bucket
        Policy policy = storage.getIamPolicy(bucketName);

        // Print Roles and its identities
        Map<Role, Set<Identity>> policyBindings = policy.getBindings();
        for (Map.Entry<Role, Set<Identity>> entry : policyBindings.entrySet()) {
            System.out.printf("Role: %s Identities: %s\n", entry.getKey(), entry.getValue());
        }
        // [END view_bucket_iam_members]
        return policy;
    }

    /** Example of creating a blob from an input stream. */
    // [TARGET create(BlobInfo, InputStream, BlobWriteOption...)]
    // [VARIABLE "my_unique_bucket"]
    // [VARIABLE "my_blob_name"]
    public static Blob createBlobFromInputStream(String bucketName, String blobName, InputStream content, String fileType) {
        // [START createBlobFromInputStream]
        BlobId blobId = BlobId.of(bucketName, blobName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(fileType).build();
        Blob blob = storage.create(blobInfo, content);
        // [END createBlobFromInputStream]
        return blob;
    }
}
