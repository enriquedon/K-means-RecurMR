package Kmeans;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import Program.Program;
import Program.initiateData;

public class kmeansReducer extends
		TableReducer<IntWritable, Text, ImmutableBytesWritable> {
	private static final int numOfColumn = Program.numOfColumn;
	private static final String Family1 = initiateData.Family1;
	private static final String Family2 = initiateData.Family2;
	List<String> centers;
	double[] newCenter = new double[numOfColumn];
	private static int numOfrow = 0;

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		numOfrow = 0;
		newCenter = new double[numOfColumn];
		for (Text val : values) {
			numOfrow++;
			calNewCenter(val.toString());
		}
		String f1 = assignNewCenter(0);
		String f2 = assignNewCenter(1);
		 System.out.println("numOfrow: " + numOfrow);
		 System.out.println(f1+f2);

		 Put put = new Put(Bytes.toBytes(key.toString()));
		 put.add(Bytes.toBytes(Family1), Bytes.toBytes("center"),
		 Bytes.toBytes(f1));
		 put.add(Bytes.toBytes(Family2), Bytes.toBytes("center"),
				 Bytes.toBytes(f2));
		 context.write(null, put);
	}

	private String assignNewCenter(int pos) {
		// !!!!!!!!!!
		double[] familyArray = new double[numOfColumn / 2];
		
		for (int i = 0; i < numOfColumn; i++) {
			newCenter[i] /= numOfrow;
		}
		String familyString = "";
		if (pos == 0) {
			System.arraycopy(newCenter, 0, familyArray, 0, numOfColumn / 2);
		} else {
			System.arraycopy(newCenter, 5, familyArray, 0, numOfColumn / 2);
		}

		for (double d : familyArray) {
			familyString += d + "	";
		}
		return familyString;
	}

	private void calNewCenter(String dataString) {
		String[] curDataInString = new String[numOfColumn];
		double[] curData = new double[numOfColumn];
		curDataInString = dataString.split("	");
		for (int i = 0; i < numOfColumn; i++) {
			newCenter[i] += Double.parseDouble(curDataInString[i]);
		}
	}
}
