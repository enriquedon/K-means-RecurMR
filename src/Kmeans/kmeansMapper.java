package Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import Program.initiateData;

public class kmeansMapper extends TableMapper<IntWritable, Text> {
	List<String> centers;
	String dataString;
	private IntWritable numOfClueter  = new IntWritable();
	private Text dataText = new Text();
	
	public void setup(Context context) throws IOException, InterruptedException {
		centers = loadFromHTable(initiateData.Table2);
		super.setup(context);
	}

	// qualifier combiner!!
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {

		dataString=new String(value.getValue(
				Bytes.toBytes(initiateData.Family1), Bytes.toBytes("data")))
				+ "	"+new String(value.getValue(
						Bytes.toBytes(initiateData.Family2), Bytes.toBytes("data")));
		
//		printcenters(dataString);
		
		numOfClueter= new IntWritable(calculaterKmeans.calkmeans(centers,dataString));
		dataText.set(dataString);
		context.write(numOfClueter, dataText);
	}

	

	private void printcenters(String dataString2) {
		System.out.println(dataString2);
		for(String s:centers){
			System.out.println(s);
		}
		System.out.println("-------------------");
		
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	};

	protected List<String> loadFromHTable(String tablename) throws IOException {
		List<String> centers = new ArrayList<String>();
		Configuration config = HBaseConfiguration.create();

		HTable Table = new HTable(config, tablename);
		Scan s = new Scan();
		ResultScanner ss = Table.getScanner(s);
		

		for (Result r : ss) {
			String centerdata="";
			for (KeyValue kv : r.raw()) {
				System.out.println(new String(kv.getValue()));
				centerdata += new String(kv.getValue()) + "	";
			}
			centers.add(centerdata);
		}
		return centers;
	}
	private void printValue(Result value) {
		System.out.print(new String(value.getValue(
				Bytes.toBytes(initiateData.Family1), Bytes.toBytes("data")))
				+ " : ");
		System.out.println(new String(value.getValue(
				Bytes.toBytes(initiateData.Family2), Bytes.toBytes("data"))));
	}
}