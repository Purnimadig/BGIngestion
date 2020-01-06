import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.common.io.Files;
import com.google.auth.oauth2.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.Channels;

public class FunctionsBigquery
{

	public static  BigQuery setServiceKey(String jsonPath) throws InterruptedException, IOException {
		
		
		GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath));

		 // Instantiate a client.
		 BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
		return bigquery;
	}
	
	
	 
public static  Long orcload(String datasetName,String location,String tableName,File orcfile,BigQuery bigquery) throws InterruptedException, IOException {

	// Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
	  // environment variable, you can explicitly load the credentials file to construct the
	  // credentials.
	
	
	System.out.println("Connecting to Dataset : "+ datasetName);
	System.out.println("Dataset Location: "+ location);
	System.out.println("Table Name  : "+ tableName);
	System.out.println("File Location : "+ orcfile);
	
	TableId tableId = TableId.of(datasetName, tableName);
    WriteChannelConfiguration writeChannelConfiguration =
        WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.orc()).build();	
	
    // The location must be specified; other fields can be auto-detected.
    JobId jobId = JobId.newBuilder().setLocation(location).build();
    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
    // Write data to writer
    
    try (OutputStream stream = Channels.newOutputStream(writer)) {
      Files.copy(orcfile, stream);
    }
    // Get load job
    Job job = writer.getJob();
    job = job.waitFor();
    LoadStatistics stats = job.getStatistics();
    return stats.getOutputRows();
	
}	

public static void runQuery(String datasetName,String tableName,BigQuery bigquery) throws InterruptedException, FileNotFoundException, IOException {

	String query = "SELECT * FROM `" +datasetName+"."+tableName+"` LIMIT 10;";
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

    // Print the results.
    for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
      for (FieldValue val : row) {
        System.out.printf("%s,", val.toString());
      }
      System.out.printf("\n");
    }
}
}	
	
	