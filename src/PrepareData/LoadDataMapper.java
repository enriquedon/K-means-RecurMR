package PrepareData;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Program.globalNameSpace;


public class LoadDataMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		int rowkey = line.hashCode();

		// qualifier,area,property
//		String[] row = rowToList(line);
		String[] row = new String[columns];
		row = line.split("	");
		
		// String rowKey = null;
//		ImmutableBytesWritable HKey = new ImmutableBytesWritable(
//				Bytes.toBytes(rowkey));
		Put HPut = new Put(Bytes.toBytes(rowkey));

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, Table1);

		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x1),
				Bytes.toBytes(row[0]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x5),
				Bytes.toBytes(row[4]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x6),
				Bytes.toBytes(row[5]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y1),
				Bytes.toBytes(row[8]));
		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y2),
				Bytes.toBytes(row[9]));
		
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x2),
				Bytes.toBytes(row[1]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x3),
				Bytes.toBytes(row[2]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x4),
				Bytes.toBytes(row[3]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x7),
				Bytes.toBytes(row[6]));
		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x8),
				Bytes.toBytes(row[7]));
		table.put(HPut);
	}

	public static int columns = globalNameSpace.numOfColumn;
	public static final String Table1 = globalNameSpace.Table1;
	public static final String Table2 = globalNameSpace.Table2;
	public static final String Family1 =globalNameSpace.Family1;
	public static final String Family2 = globalNameSpace.Family2;
	public static final String x1=globalNameSpace.x1;
	public static final String x2=globalNameSpace.x2;
	public static final String x3=globalNameSpace.x3;
	public static final String x4=globalNameSpace.x4;
	public static final String x5=globalNameSpace.x5;
	public static final String x6=globalNameSpace.x6;
	public static final String x7=globalNameSpace.x7;
	public static final String x8=globalNameSpace.x8;
	public static final String y1=globalNameSpace.y1;
	public static final String y2=globalNameSpace.y2;

}
