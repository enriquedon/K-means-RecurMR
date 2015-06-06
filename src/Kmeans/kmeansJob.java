package Kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import Program.globalNameSpace;

public class kmeansJob {
	public void runKmeans() throws IOException, ClassNotFoundException,
			InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		long counter = 1;
		Job job = new Job(conf);

		for (int i = 0; counter > 0; i++) {
			conf = HBaseConfiguration.create();
			job = new Job(conf, "Kmeans iteration: " + i);
			System.out.println(job.getJobName());
			job.setJarByClass(kmeansMapper.class); // class that contains mapper
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			// List<Scan> scans = new ArrayList<Scan>();
			// scans = addOneScan(scans, initiateData.Table1);
			// scans = addOneScan(scans, initiateData.Table2);

			TableMapReduceUtil.initTableMapperJob(Table1, scan,
					kmeansMapper.class, IntWritable.class, Text.class, job);
			// job.setCombinerClass(kmeansCombiner.class);
			TableMapReduceUtil.initTableReducerJob(Table2, kmeansReducer.class,
					job);
			// job.setNumReduceTasks(numOfclusters);

			if (!job.waitForCompletion(true)) {
				throw new IOException("error with job!");
			}

			counter = job.getCounters()
					.findCounter(kmeansReducer.UpdateCounter.UPDATED)
					.getValue();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
		}
		clearInterdata(conf);
	}

	private void clearInterdata(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(tempOutput))) {
			fs.delete(new Path(tempOutput), true);
		}
		// HBaseAdmin admin = new HBaseAdmin(conf);
		// if (admin.tableExists(Table1)) {
		// if (admin.isTableEnabled(Table1)) {
		// admin.disableTable(Table1);
		// }
		// admin.deleteTable(Table1);
		// System.out.println("delete "+Table1);
		// }
	}

	public static final String Table1 = globalNameSpace.Table1;
	public static final String Table2 = globalNameSpace.Table2;
	public static final String tempOutput = globalNameSpace.tempOutput;
	public static final int numOfclusters = globalNameSpace.numOfclusters;
}
