package PrepareData;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import Kmeans.kmeansReducer;
import Program.Program;
import Program.globalNameSpace;

public class initiateData {

	public void initiateHTables(Path input) throws ClassNotFoundException,
			IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		checkOutputFolder(conf);
		create2HTable(conf);
		loadDataJob(conf, input);
		loadInitialCenter(conf, numOfclusters);
	}

	private void create2HTable(Configuration conf) throws IOException {
		// Instantiating configuration class
		// Configuration con = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(conf);

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

	@SuppressWarnings("deprecation")
	private void loadDataJob(Configuration conf, Path input)
			throws IOException, ClassNotFoundException, InterruptedException {
		// Configuration conf = HBaseConfiguration.create();
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
		setNumOfRow(job.getCounters()
				.findCounter(LoadDataMapper.RowCounter.numOfRow).getValue());
	}

	private void loadInitialCenter(Configuration conf, int numOfclusters)
			throws IOException {

		// Configuration conf = HBaseConfiguration.create();
		HTable dataTable = new HTable(conf, Table1);
		HTable centerTable = new HTable(conf, Table2);
		Scan s = new Scan();
		ResultScanner ss = dataTable.getScanner(s);
		Put HPut = null;
		// ImmutableBytesWritable HKey;

//		int numOfRow = 768;
		int numOfRow = getNumOfRow();
		if (numOfclusters > numOfRow) {
			System.err.println("numOfclusters: " + "numOfclusters"
					+ " > numOfRow: " + numOfRow);
		}
		randPickCenters(numOfclusters, numOfRow);

		for (int i = 0; numOfclusters > 0; i++) {
			Result r = ss.next();
			if (!NumOfCenter.contains(i)) {
				continue;
			}
			System.out.println("#" + i);
			HPut = new Put(Bytes.toBytes(Integer.toString(numOfclusters - 1)));
			for (KeyValue kv : r.raw()) {
				// System.out.println(new String(kv.getRow()) + "!!!!!!!!!!");
				// System.out.println(new String(kv.getKeyString()) + ":");
				// System.out.println(new String(kv.getQualifier()) + " ");
				// System.out.print(kv.getTimestamp() + " ");
				// System.out.println(new String(kv.getValue()));
				HPut.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
			}
			centerTable.put(HPut);
			numOfclusters--;
		}
	}

	private void randPickCenters(int numOfclusters, int numOfRow) {
		int num = randInt(0, numOfRow);
		for (int i = 0; i < numOfclusters; i++) {
			while (NumOfCenter.contains(num)) {
				num = randInt(0, numOfRow);
			}
			NumOfCenter.add(num);
		}
	}

	private int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	public static int getNumOfRow() {
		return NumOfRow;
	}

	private static void setNumOfRow(long numOfRow) {
		if (numOfRow < Integer.MIN_VALUE || numOfRow > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(numOfRow
					+ " WOW!!!So many rows.");
		}
		NumOfRow = (int) numOfRow;
	}

	private void checkOutputFolder(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("output"))) {
			fs.delete(new Path("output"), true);
			System.out.println("output existed，now deleted！！！");
		}
	}
	
	public static final String Table1 = globalNameSpace.Table1;
	public static final String Table2 = globalNameSpace.Table2;
	public static final String Family1 = globalNameSpace.Family1;
	public static final String Family2 = globalNameSpace.Family2;
	public static final int numOfclusters = globalNameSpace.numOfclusters;
	private static int NumOfRow = 0;
	private static Set<Integer> NumOfCenter = new HashSet<Integer>();
}
