package SearchDemo;

import com.aliyun.fc.runtime.*;
import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Maps;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.*;
import java.util.*;

public class EventHandler implements StreamRequestHandler {

    private static String ossEndpoint = "YourOSSEndpoint";
    private static String openSearchAppName = "YourOpenSearchAppName";
    private static String openSearchHost = "YourOpenSearchHost";
    private static String openSearchTableName = "YourOpenSearchTableName";
    private static String accessKeyId = "YourAccessKeyId";
    private static String accessKeySecret = "YourAccessSecretId";
    private static String docUrlFormat = "http://%s.%s/%s";

    private static String[] addEventArray = {"ObjectCreated:PutObject", "ObjectCreated:PostObject"};
    private static List<String> addEventList = Arrays.asList(addEventArray);
    private static String[] updateEventArray = {"ObjectCreated:AppendObject"};
    private static List<String> updateEventList = Arrays.asList(updateEventArray);
    private static String[] deleteEventArray = {"ObjectRemoved:DeleteObject", "ObjectRemoved:DeleteObjects"};
    private static List<String> deleteEventList = Arrays.asList(deleteEventArray);


    @Override
    public void handleRequest(
            InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        /*
         * Preparation
         * Init logger, oss client, open search document client.
         */
        FunctionComputeLogger fcLogger = context.getLogger();
        OSSClient ossClient = getOSSClient(context);
        DocumentClient documentClient = getDocumentClient();

        /*
         * Step 1
         * Read oss event from input stream.
         */
        JSONObject ossEvent;
        StringBuilder inputBuilder = new StringBuilder();
        BufferedReader streamReader = null;
        try {
            streamReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = streamReader.readLine()) != null) {
                inputBuilder.append(line);
            }
            fcLogger.info("Read object event success.");
        } catch(Exception ex) {
            fcLogger.info(ex.getMessage());
        } finally{
            closeQuietly(streamReader, fcLogger);
        }
        ossEvent = JSONObject.fromObject(inputBuilder.toString());
        fcLogger.info("Getting event: " + ossEvent.toString());

        /*
         * Step 2
         * Loop every events in oss event, and generate structured docs in json format.
         */
        JSONArray events = ossEvent.getJSONArray("events");
        for(int i = 0; i < events.size(); i++) {

            // Get event name, source, oss object.
            JSONObject event = events.getJSONObject(i);
            String eventName = event.getString("eventName");
            JSONObject oss = event.getJSONObject("oss");

            // Get bucket name and file name for file identifier.
            JSONObject bucket = oss.getJSONObject("bucket");
            String bucketName = bucket.getString("name");
            JSONObject object = oss.getJSONObject("object");
            String fileName = object.getString("key");

            // Prepare fields for commit to open search
            Map<String, Object> structuredDoc = Maps.newLinkedHashMap();
            BufferedReader objectReader = null;
            UUID uuid = new UUID(bucketName.hashCode(), fileName.hashCode());
            structuredDoc.put("identifier", uuid);

            try {
                // For delete event, delete by identifier
                if (deleteEventList.contains(eventName)) {
                    documentClient.remove(structuredDoc);
                } else {
                    OSSObject ossObject = ossClient.getObject(bucketName, fileName);

                    // Non delete event, read file content and more field you need
                    StringBuilder fileContentBuilder = new StringBuilder();
                    objectReader = new BufferedReader(
                            new InputStreamReader(ossObject.getObjectContent()));

                    String contentLine;
                    while ((contentLine = objectReader.readLine()) != null) {
                        fileContentBuilder.append('\n' + contentLine);
                    }
                    fcLogger.info("Read object content success.");

                    // You can put more fields according to your scenario
                    structuredDoc.put("title", fileName);
                    structuredDoc.put("content", fileContentBuilder.toString());
                    structuredDoc.put("subject", String.format(docUrlFormat, bucketName, ossEndpoint, fileName));

                    if (addEventList.contains(eventName)) {
                        documentClient.add(structuredDoc);
                    } else if (updateEventList.contains(eventName)) {
                        documentClient.update(structuredDoc);
                    }
                }
            } catch (Exception ex) {
                fcLogger.info(ex.getMessage());
            } finally {
                closeQuietly(objectReader, fcLogger);
            }
        }

        /*
         * Step 3
         * Commit json docs string to open search
         */
        try {
            OpenSearchResult osr = documentClient.commit(openSearchAppName, openSearchTableName);
            if(osr.getResult().equalsIgnoreCase("true")) {
                fcLogger.info("OSS Object commit to OpenSearch success.");
            } else {
                fcLogger.info("Fail to commit to OpenSearch.");
            }
        } catch (OpenSearchException ex) {
            fcLogger.info(ex.getMessage());
        } catch (OpenSearchClientException ex) {
            fcLogger.info(ex.getMessage());
        }
    }

    protected OSSClient getOSSClient(Context context) {
        Credentials creds = context.getExecutionCredentials();
        return new OSSClient(
                ossEndpoint, creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    protected DocumentClient getDocumentClient() {
        OpenSearch openSearch = new OpenSearch(accessKeyId, accessKeySecret, openSearchHost);
        OpenSearchClient serviceClient = new OpenSearchClient(openSearch);
        return new DocumentClient(serviceClient);
    }

    protected void closeQuietly(BufferedReader reader, FunctionComputeLogger fcLogger) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception ex) {
            fcLogger.info(ex.getMessage());
        }
    }
}