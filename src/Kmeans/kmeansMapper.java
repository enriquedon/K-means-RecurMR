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
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import Program.globalNameSpace;
import Program.initiateData;

public class kmeansMapper extends TableMapper<IntWritable, Text> {
	List<String> centers;
	String dataString;
	private IntWritable numOfClueter = new IntWritable();
	private Text dataText = new Text();

	public void setup(Context context) throws IOException, InterruptedException {
		centers = loadFromHTable(initiateData.Table2);
		super.setup(context);
	}

	// qualifier combiner!!
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		dataString = makeDataString(value);

//		printcenters(dataString);

		numOfClueter = new IntWritable(calculaterKmeans.calkmeans(centers,
				dataString));
		dataText.set(dataString);
		context.write(numOfClueter, dataText);
	}

	private String makeDataString(Result value) {
		// TODO Auto-generated method stub
		return new String(value.getValue(Bytes.toBytes(Family1),
				Bytes.toBytes(x1)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family1),
						Bytes.toBytes(x5)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family1),
						Bytes.toBytes(x6)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family1),
						Bytes.toBytes(y1)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family1),
						Bytes.toBytes(y2)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family2),
						Bytes.toBytes(x2)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family2),
						Bytes.toBytes(x3)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family2),
						Bytes.toBytes(x4)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family2),
						Bytes.toBytes(x7)))
				+ "	"
				+ new String(value.getValue(Bytes.toBytes(Family2),
						Bytes.toBytes(x8)));
	}

	private void printcenters(String dataString2) {
		System.out.println(dataString2);
		System.out.println("-------------------");

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	};

	protected List<String> loadFromHTable(String tablename) throws IOException {
		List<String> centers = new ArrayList<String>();
		Configuration config = HBaseConfiguration.create();

		HTable Table = new HTable(config, tablename);
		Scan s = new Scan();
		ResultScanner ss = Table.getScanner(s);

		for (Result r : ss) {
			String centerdata = "";
			for (KeyValue kv : r.raw()) {
//				System.out.println(new String(kv.getQualifier()));
				centerdata += new String(kv.getValue()) + "	";
			}
			centers.add(centerdata);
		}
		return centers;
	}


	public static final String Family1 = globalNameSpace.Family1;
	public static final String Family2 = globalNameSpace.Family2;
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