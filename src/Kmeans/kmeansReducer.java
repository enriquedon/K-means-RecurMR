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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import PrepareData.initiateData;
import Program.globalNameSpace;

public class kmeansReducer extends
		TableReducer<IntWritable, Text, ImmutableBytesWritable> {

	List<List<Double>> centers;
	double[] newCenter;
	String[] newCenterInString;
	private static int numOfrow;

	public void setup(Context context) throws IOException, InterruptedException {
		centers = loadFromHTable(initiateData.Table2);
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		numOfrow = 0;
		newCenter = new double[numOfColumn];
		newCenterInString = new String[numOfColumn];
		for (Text val : values) {
			numOfrow++;
			calNewCenter(val.toString());
		}
		System.out.println("numOfrow: " + numOfrow);
		assignNewCenter();
		
		// System.out.println(f1 + f2);
		ImmutableBytesWritable HKey = new ImmutableBytesWritable(
				Bytes.toBytes(key.toString()));
		Put HPut = new Put(Bytes.toBytes(key.toString()));
		
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x1),
				Bytes.toBytes(newCenterInString[0]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x5),
				Bytes.toBytes(newCenterInString[1]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x6),
				Bytes.toBytes(newCenterInString[2]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y1),
				Bytes.toBytes(newCenterInString[3]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y2),
				Bytes.toBytes(newCenterInString[4]));

		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x2),
				Bytes.toBytes(newCenterInString[5]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x3),
				Bytes.toBytes(newCenterInString[6]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x4),
				Bytes.toBytes(newCenterInString[7]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x7),
				Bytes.toBytes(newCenterInString[8]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x8),
				Bytes.toBytes(newCenterInString[9]));

		if (!isConverged(key)) {
			context.getCounter(UpdateCounter.UPDATED).increment(1);
			context.write(HKey, HPut);
		}
		System.out.println();

	}

	//not been divided by numOfrow yet.
	private void calNewCenter(String dataString) {
		String[] curDataInString = new String[numOfColumn];
		double[] curData = new double[numOfColumn];
		curDataInString = dataString.split("	");
		for (int i = 0; i < numOfColumn; i++) {
			newCenter[i] += Double.parseDouble(curDataInString[i]);
		}
	}
	
	private void assignNewCenter() {
		for (int i = 0; i < numOfColumn; i++) {
			System.out.print(newCenter[i]+",");
			newCenter[i] /= numOfrow;
			newCenterInString[i] = Double.toString(newCenter[i]);
		}
	}
	
	private boolean isConverged(IntWritable key) {
		int numOfCenter = Integer.parseInt(key.toString());
		System.out.println("Cluster #"+numOfCenter);
		List<Double> center = centers.get(numOfCenter);
		double dist = 0;

		for (int i = 0; i < numOfColumn; i++) {
			dist += Math.pow(center.get(i) - newCenter[i], 2);
		}
		

		if (dist > 0.1) {
			System.out.println("dist: " + dist);
			PrintArray(newCenter);
			return false;
		}
		PrintArray(newCenter);
		return true;
	}

	protected List<List<Double>> loadFromHTable(String tablename)
			throws IOException {
		List<List<Double>> centers = new ArrayList<List<Double>>();
		List<Double> center = new ArrayList<Double>();
		Configuration config = HBaseConfiguration.create();

		HTable Table = new HTable(config, tablename);
		Scan s = new Scan();
		ResultScanner ss = Table.getScanner(s);

		for (Result r : ss) {
			center = new ArrayList<Double>();
			for (KeyValue kv : r.raw()) {
				// System.out.println(new String(kv.getQualifier()));
				center.add(Double.parseDouble(new String(kv.getValue())));
			}
			centers.add(center);
		}
		return centers;
	}

	private void PrintArray(double[] newCenter2) {
		System.out.print("CENTER:   [");
		for (double i : newCenter2) {
			System.out.print(i + " ");
		}
		System.out.println("]");
	}

	private void PrintList(List<Double> center) {
		System.out.print("former center:[");
		for (double i : center) {
			
			System.out.print(i + " ");
		}
		System.out.println("]");
	}

	public enum UpdateCounter {
		UPDATED
	}

	private static final int numOfColumn = globalNameSpace.numOfColumn;
	private static final String Family1 = globalNameSpace.Family1;
	private static final String Family2 = globalNameSpace.Family2;
	public static final String x1 = globalNameSpace.x1;
	public static final String x2 = globalNameSpace.x2;
	public static final String x3 = globalNameSpace.x3;
	public static final String x4 = globalNameSpace.x4;
	public static final String x5 = globalNameSpace.x5;
	public static final String x6 = globalNameSpace.x6;
	public static final String x7 = globalNameSpace.x7;
	public static final String x8 = globalNameSpace.x8;
	public static final String y1 = globalNameSpace.y1;
	public static final String y2 = globalNameSpace.y2;
}
