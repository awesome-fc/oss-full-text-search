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

    private static final String OSS_ENDPOINT = "YourOSSEndpoint";
    private static final String OPENSEARCH_APP_NAME = "YourOpenSearchAppName";
    private static final String OPENSEARCH_HOST = "YourOpenSearchHost";
    private static final String OPENSEARCH_TABLE_NAME = "YourOpenSearchTableName";
    private static final String ACCESS_KEY_ID = "YourAccessKeyId";
    private static final String ACCESS_KEY_SECRET = "YourAccessSecretId";
    private static final String DOC_URL_FORMAT = "http://%s.%s/%s";

    private static final List<String> addEventList = Arrays.asList(
            "ObjectCreated:PutObject", "ObjectCreated:PostObject");
    private static final List<String> updateEventList = Arrays.asList(
            "ObjectCreated:AppendObject");
    private static final List<String> deleteEventList = Arrays.asList(
            "ObjectRemoved:DeleteObject", "ObjectRemoved:DeleteObjects");

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
            fcLogger.error(ex.getMessage());
            return;
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
                    structuredDoc.put("subject", String.format(DOC_URL_FORMAT, bucketName, OSS_ENDPOINT, fileName));

                    if (addEventList.contains(eventName)) {
                        documentClient.add(structuredDoc);
                    } else if (updateEventList.contains(eventName)) {
                        documentClient.update(structuredDoc);
                    }
                }
            } catch (Exception ex) {
                fcLogger.error(ex.getMessage());
                return;
            } finally {
                closeQuietly(objectReader, fcLogger);
            }
        }

        /*
         * Step 3
         * Commit json docs string to open search
         */
        try {
            OpenSearchResult osr = documentClient.commit(OPENSEARCH_APP_NAME, OPENSEARCH_TABLE_NAME);
            if(osr.getResult().equalsIgnoreCase("true")) {
                fcLogger.info("OSS Object commit to OpenSearch success.");
            } else {
                fcLogger.info("Fail to commit to OpenSearch.");
            }
        } catch (OpenSearchException ex) {
            fcLogger.error(ex.getMessage());
            return;
        } catch (OpenSearchClientException ex) {
            fcLogger.error(ex.getMessage());
            return;
        }
    }

    protected OSSClient getOSSClient(Context context) {
        Credentials creds = context.getExecutionCredentials();
        return new OSSClient(
                OSS_ENDPOINT, creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    protected DocumentClient getDocumentClient() {
        OpenSearch openSearch = new OpenSearch(ACCESS_KEY_ID, ACCESS_KEY_SECRET, OPENSEARCH_HOST);
        OpenSearchClient serviceClient = new OpenSearchClient(openSearch);
        return new DocumentClient(serviceClient);
    }

    protected void closeQuietly(BufferedReader reader, FunctionComputeLogger fcLogger) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception ex) {
            fcLogger.error(ex.getMessage());
        }
    }
}