package Program;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import PrepareData.LoadDataMapper;

public class initiateData {
	public static final String Table1 = "SchemaData";
	public static final String Table2 = "Center";
	public static final String Family1 = "Area";
	public static final String Family2 = "Property";

	void initiateHTables(Path input, int numOfclusters)
			throws ClassNotFoundException, IOException, InterruptedException {
		loadDataInHBase(input);
		loadInitialCluster(numOfclusters);

	}

	private void loadInitialCluster(int numOfclusters) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable dataTable = new HTable(conf, Table1);
		HTable centerTable = new HTable(conf, Table2);
		Scan s = new Scan();
		ResultScanner ss = dataTable.getScanner(s);
		Put HPut = null;
		ImmutableBytesWritable HKey;

		int i = 0;
		for (Result r : ss) {
			if (i == numOfclusters) {
				break;
			}
			i++;
			HKey = new ImmutableBytesWritable(Bytes.toBytes(i));
			HPut = new Put(Bytes.toBytes(i));
			for (KeyValue kv : r.raw()) {
				// System.out.print(new String(kv.getRow()) + " ");
				// System.out.println(new String(kv.getKeyString()) + ":");
				// System.out.println(new String(kv.getQualifier()) + " ");
				// System.out.print(kv.getTimestamp() + " ");
//				 System.out.println(new String(kv.getValue()));
				HPut.add(kv.getFamily(), Bytes.toBytes("center"), kv.getValue());
			}
			centerTable.put(HPut);
		}
	}

	@SuppressWarnings("deprecation")
	private void loadDataInHBase(Path input) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf, Table1);
		Job job = new Job(conf, "HBase_Bulk_loader");
		FileInputFormat.setInputPaths(job, input);
		Path output = new Path("output");
		FileOutputFormat.setOutputPath(job, output);
		job.setJarByClass(Program.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		// job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setMapperClass(LoadDataMapper.class);

		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		// DistributedCache.addFileToClassPath(new
		// Path("file:///home/dy/EECS219/hadoop-2.7.0/code.jar"), conf);
		job.waitForCompletion(true);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	// private void loadDataInHBase(Path input) {
	// JobConf conf = new JobConf(getConf(), Program.class);
	// conf.setJobName("Prepare data in Hbase");
	//
	// Scan scan = new Scan();
	// scan.setCaching(500); // 1 is the default in Scan, which will be bad for
	// // MapReduce jobs
	// scan.setCacheBlocks(false); // don't set to true for MR jobs
	// // set other scan attrs
	// FileInputFormat.addInputPath(conf, input);
	//
	// conf.setSpeculativeExecution(false);
	// conf.setReduceSpeculativeExecution(false);
	// conf.setInputFormat(TextInputFormat.class);
	//
	// // conf.setOutputFormat( HFileOutputFormat.class);
	// conf.setOutputKeyClass(ImmutableBytesWritable.class);
	// conf.setOutputValueClass(Put.class);
	// conf.setMapperClass(LoadDataMapper.class);
	//
	//
	// // TableMapReduceUtil.initTableMapJob(
	// // Table1,
	// // scan,
	// // LoadDataMapper.class,
	// // Text.class, Result.class, conf);
	// conf.setNumReduceTasks(0);
	//
	// }

	void create2HTable() throws IOException {
		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor1 = new HTableDescriptor(Table1);
		HTableDescriptor tableDescriptor2 = new HTableDescriptor(Table2);
		// Adding column families to table descriptor
		tableDescriptor1.addFamily(new HColumnDescriptor(Family1));
		tableDescriptor1.addFamily(new HColumnDescriptor(Family2));

		tableDescriptor2.addFamily(new HColumnDescriptor(Family1));
		tableDescriptor2.addFamily(new HColumnDescriptor(Family2));

		// Execute the table through admin
		if (admin.tableExists(Table1)) {
			admin.disableTable(Table1);
			admin.deleteTable(Table1);
			System.out.println(Table1 + " exists and deleted");
		}
		if (admin.tableExists(Table2)) {
			admin.disableTable(Table2);
			admin.deleteTable(Table2);
			System.out.println(Table2 + " exists and deleted");
		}
		admin.createTable(tableDescriptor1);
		admin.createTable(tableDescriptor2);
	}
}
