package Program;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
		setNumOfclusters(Integer.valueOf(args[1]));

		initiateData initdata = new initiateData();
		 initdata.create2HTable();
		 initdata.initiateHTables(input, numOfclusters);

		runKmeans(numOfclusters);
		return 0;
	}

	private void runKmeans(int numOfclusters) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		// counter from the previous running import job
		long counter = 1;
		Job job = new Job(config);
		while (counter>0) {
			config = HBaseConfiguration.create();
			job = new Job(config, "Kmeans iteration: " + counter/numOfclusters);

			System.out.println("counter1: " + counter);
			job.setJarByClass(kmeansMapper.class); // class that contains mapper
			Scan scan = new Scan();
			scan.setCaching(1111);
			scan.setCacheBlocks(false);
			// List<Scan> scans = new ArrayList<Scan>();
			// scans = addOneScan(scans, initiateData.Table1);
			// scans = addOneScan(scans, initiateData.Table2);

			// set other scan attrs
			TableMapReduceUtil.initTableMapperJob(Table1, scan,
					kmeansMapper.class, IntWritable.class, Text.class, job);
			TableMapReduceUtil.initTableReducerJob(Table2, kmeansReducer.class,
					job);
			// job.setNumReduceTasks(numOfclusters);

			// variable to keep track of the recursion depth

			if (!job.waitForCompletion(true)) {
				throw new IOException("error with job!");
			}

			counter = job.getCounters()
					.findCounter(kmeansReducer.UpdateCounter.UPDATED)
					.getValue();
			System.out.println("counter2: " + counter);
		}

	}

	private void setNumOfclusters(int numOfclusters2) {
		numOfclusters = numOfclusters2;

	}

	public static final int numOfColumn = globalNameSpace.numOfColumn;
	public static int numOfclusters = globalNameSpace.numOfclusters;
	public static final long iteration = globalNameSpace.iteration;
	public static final String Table1 = globalNameSpace.Table1;
	public static final String Table2 = globalNameSpace.Table2;
}
