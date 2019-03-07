
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;

public class GetObject {

    public static void main(String[] args) throws Exception{
        GetObject test = new GetObject("Categories.csv");
        List<List<String>> res = test.getContentOfObject();
        for (List<String> row: res) {
//            System.out.println(row.get(0) + "\t\t" + row.get(1) + "\t\t" + row.get(2));
        }
        test.getKeys();
    }

    private String bucketName = null;
    private String key = null;
    private String region = null;

    public GetObject(String bucketName, String key, String region) {
        this.bucketName = bucketName;
        this.key = key;
        this.region = region;
    }

    public GetObject(String bucketName, String key) {
        this.bucketName = bucketName;
        this.key = key;
        this.region = Config.REGION;
    }

    public GetObject(String key) {
        this.bucketName = Config.BUCKET_NAME;
        this.key = key;
        this.region = Config.REGION;
    }

    public void getKeys() throws Exception{
        String clientRegion = this.region;
        String bucketName = "cs527";
        String key = this.key;
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        System.out.println("Listing objects");

        // maxKeys is set to 2 to demonstrate the use of
        // ListObjectsV2Result.getNextContinuationToken()
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
        ListObjectsV2Result result;

        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
            }
            // If there are more than maxKeys keys in the bucket, get a continuation token
            // and list the next objects.
            String token = result.getNextContinuationToken();
            System.out.println("Next Continuation Token: " + token);
            req.setContinuationToken(token);
        } while (result.isTruncated());
    }

    public List<List<String>> getContentOfObject() throws IOException {
        String clientRegion = this.region;
        String bucketName = this.bucketName;
        String key = this.key;

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Get an object and print its contents.
            System.out.println("Downloading an object");
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());

            // Get a range of bytes from an object and print the bytes.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
            System.out.println("Printing bytes retrieved.");
            System.out.println(getTextInputStream(objectPortion.getObjectContent()));

            // Get an entire object, overriding the specified response headers, and print the object's content.
            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            return getListOfStringInputStream(headerOverrideObject.getObjectContent());
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
        return new ArrayList<List<String>>();
    }

    private String getTextInputStream(InputStream input) throws IOException {
        StringBuilder sb = new StringBuilder();
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line.trim() + "\n");
        }
        return sb.toString();
    }

    private List<List<String>> getListOfStringInputStream(InputStream input) throws IOException {
        String tmp = getTextInputStream(input);
        List<List<String>> res = new ArrayList<List<String>>();
        // Read the text input stream one line at a time and parse each line.
        CSVReader reader = new CSVReader(new StringReader(tmp), ',', '"');
        String[] line;
        while ((line = reader.readNext()) != null) {
            res.add(Arrays.asList(line));
        }
        return res;



    }

}

