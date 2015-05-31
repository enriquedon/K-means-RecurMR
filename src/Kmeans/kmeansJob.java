package Kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import Program.globalNameSpace;

public class kmeansJob {
	public void runKmeans() throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		long counter = 1;
		Job job = new Job(conf);
		while (counter > 0) {
			conf = HBaseConfiguration.create();
			job = new Job(conf, "Kmeans iteration: " + counter
					/ numOfclusters);

			job.setJarByClass(kmeansMapper.class); // class that contains mapper
			Scan scan = new Scan();
			scan.setCaching(1111);
			scan.setCacheBlocks(false);
			// List<Scan> scans = new ArrayList<Scan>();
			// scans = addOneScan(scans, initiateData.Table1);
			// scans = addOneScan(scans, initiateData.Table2);

			TableMapReduceUtil.initTableMapperJob(Table1, scan,
					kmeansMapper.class, IntWritable.class, Text.class, job);
//			job.setCombinerClass(kmeansCombiner.class);
			TableMapReduceUtil.initTableReducerJob(Table2, kmeansReducer.class,
					job);
			// job.setNumReduceTasks(numOfclusters);

			if (!job.waitForCompletion(true)) {
				throw new IOException("error with job!");
			}

			counter = job.getCounters()
					.findCounter(kmeansReducer.UpdateCounter.UPDATED)
					.getValue();
		}
		clearOutputFolder(conf);
	}
	
	private void clearOutputFolder(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("output"))) {
			fs.delete(new Path("output"), true);
		}
	}
	
	public static final String Table1 = globalNameSpace.Table1;
	public static final String Table2 = globalNameSpace.Table2;
	public static final int numOfclusters = globalNameSpace.numOfclusters;
}
