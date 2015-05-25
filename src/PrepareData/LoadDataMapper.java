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

import Program.*;

public class LoadDataMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	public static int columns = 10;

	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		int rowkey = line.hashCode();

		System.out.println(line);

		// qualifier,area,property
		String[] row = rowToList(line);
		// String rowKey = null;
		ImmutableBytesWritable HKey = new ImmutableBytesWritable(
				Bytes.toBytes(rowkey));
		Put HPut = new Put(Bytes.toBytes(rowkey));
		// HPut.add(Bytes.toBytes(Program.Family1), Bytes.toBytes(row[0]),
		// Bytes.toBytes(row[1]));
		// HPut.add(Bytes.toBytes(Program.Family2), Bytes.toBytes(row[0]),
		// Bytes.toBytes(row[2]));
		// context.write(HKey, HPut);

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, initiateData.Table1);

		HPut.add(Bytes.toBytes(initiateData.Family1), Bytes.toBytes(row[0]),
				Bytes.toBytes(row[1]));
		HPut.add(Bytes.toBytes(initiateData.Family2), Bytes.toBytes(row[0]),
				Bytes.toBytes(row[2]));
		table.put(HPut);
	}

	private String[] rowToList(String line) {
		String[] rowa = new String[columns];
		String[] rowb = new String[3];
		rowa = line.split("	");
		String area = rowa[0] + "	" + rowa[4] + "	" + rowa[5] + "	" + rowa[8]
				+ "	" + rowa[9];
		String property = rowa[1] + "	" + rowa[2] + "	" + rowa[3] + "	"
				+ rowa[6] + "	" + rowa[7];
		rowb[0] = line;
		rowb[1] = area;
		rowb[2] = property;
		return rowb;
	}
}
