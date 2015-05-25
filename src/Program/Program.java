package Program;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Kmeans.kmeansMapper;
import Kmeans.kmeansReducer;
import PrepareData.LoadDataMapper;

public class Program extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new Program(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Path input = new Path(args[0]);
		int numOfclusters = Integer.valueOf(args[1]);

		initiateData initdata = new initiateData();
		initdata.create2HTable();
		initdata.initiateHTables(input, numOfclusters);
		runKmeans(numOfclusters);

		return 0;
	}

	private void runKmeans(int numOfclusters) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Kmeans");
		job.setJarByClass(kmeansMapper.class);    // class that contains mapper

		
		List<Scan> scans = new ArrayList<Scan>();
		scans = addOneScan(scans,initiateData.Table1); 
		scans = addOneScan(scans,initiateData.Table2); 
      
		

		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob(scans, kmeansMapper.class, Text.class, Text.class, job); 
		TableMapReduceUtil.initTableReducerJob(
				initiateData.Table2,      // output table
				kmeansReducer.class,             // reducer class
			job);
		job.setNumReduceTasks(numOfclusters);

		if (!job.waitForCompletion(true)) {
		    throw new IOException("error with job!");
		}
	}

	private List<Scan> addOneScan(List<Scan> scans, String table) {
		Scan scan = new Scan();
		scan.setCaching(5000);  
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getBytes()); 
		 scans.add(scan);
		return scans;
	}


	private Scan setScan(String table1) {
		Scan scan = new Scan();
		return null;
	}

}
