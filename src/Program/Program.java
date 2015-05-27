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
	public static final int numOfColumn = 10;
	public static int numOfclusters = 1;

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
//		initdata.create2HTable();
//		initdata.initiateHTables(input, numOfclusters);
		runKmeans(numOfclusters);

		return 0;
	}

	private void runKmeans(int numOfclusters) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		Job job = new Job(config, "Kmeans");
		job.setJarByClass(kmeansMapper.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(1111);
		scan.setCacheBlocks(false);
		// List<Scan> scans = new ArrayList<Scan>();
		// scans = addOneScan(scans, initiateData.Table1);
		// scans = addOneScan(scans, initiateData.Table2);

		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob(initiateData.Table1, scan,
				kmeansMapper.class, IntWritable.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(initiateData.Table2,
				kmeansReducer.class, // reducer class
				job);
		job.setNumReduceTasks(numOfclusters);

		if (!job.waitForCompletion(true)) {
			throw new IOException("error with job!");
		}
	}

	private void setNumOfclusters(int numOfclusters2) {
		numOfclusters = numOfclusters2;

	}

	private List<Scan> addOneScan(List<Scan> scans, String table) {
		Scan scan = new Scan();
		scan.setCaching(5000);
		scan.setCacheBlocks(false);
		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getBytes());
		scans.add(scan);
		return scans;
	}

}
