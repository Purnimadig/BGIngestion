import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.util.concurrent.TimeoutException;

/**
 * A snippet for Google Cloud BigQuery showing how to get a BigQuery table or create it if it does
 * not exist. The snippet also starts a BigQuery job to load data into the table from a Cloud
 * Storage blob and wait until the job completes.
 */
public class SimpleApp {

  public static void main(String... args) throws InterruptedException, TimeoutException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    TableId tableId = TableId.of("Sample", "employee");
    Table table = bigquery.getTable(tableId);
    if (table == null) {
      System.out.println("Creating table " + tableId);
      Field integerField = Field.of("rollno", LegacySQLTypeName.INTEGER);
      Schema schema = Schema.of(integerField);
      table = bigquery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
    }
		
		  System.out.println("Loading data into table " + tableId); Job loadJob =
		  table.load(FormatOptions.csv(), "gs://bucket/path"); loadJob =
		  loadJob.waitFor(); if (loadJob.getStatus().getError() != null) {
		  System.out.println("Job completed with errors"); } else {
		  System.out.println("Job succeeded"); }
		 
  }
}