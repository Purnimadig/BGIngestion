import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;

import com.google.cloud.bigquery.BigQuery;

@SuppressWarnings("unused")
public class Runner
{

	public static void main(String[] args) throws IOException {
		 String datasetName = "Sample"; 
		 String tableName = "orc_test_data";
		 String location = "US";
		 File orcfile = new File("C:\\Users\\purnima\\eclipse-workspace\\BQuery\\userdata5_orc");
		 String jsonPath = "C:\\Users\\purnima\\Downloads\\bgkey.json";
		 
	try {
		long outputRows = 0;
	   //to connect to bigquery to set service account key
		BigQuery bqObject=FunctionsBigquery.setServiceKey(jsonPath);
		 System.out.println("Authenticated Successfully.. ");
		 System.out.println("Creating table....");
		outputRows = FunctionsBigquery.orcload(datasetName,location,tableName,orcfile,bqObject);
		System.out.println("Table created Successfully");
		System.out.println("Number of records written are : "+ outputRows);
		System.out.println("*************************************************************");
		System.out.println("Selecting records from table...");
		FunctionsBigquery.runQuery(datasetName, tableName,bqObject);
	} catch (InterruptedException e) {
		e.printStackTrace();
	}

}
}
