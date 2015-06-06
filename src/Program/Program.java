package Program;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Kmeans.kmeansJob;
import Kmeans.kmeansMapper;
import Kmeans.kmeansReducer;
import PrepareData.LoadDataMapper;
import PrepareData.initiateData;

public class Program extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new Program(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Path input = new Path(args[0]);
		globalNameSpace.setNumOfclusters(Integer.valueOf(3));
		

		initiateData initdata = new initiateData();
		initdata.initiateHTables(input);
		
		kmeansJob kJob=new kmeansJob();
		kJob.runKmeans();
		return 0;
	}

	public static final int numOfColumn = globalNameSpace.numOfColumn;
	
}
