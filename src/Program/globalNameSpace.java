package Program;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class globalNameSpace {
	public static final int numOfColumn = 10;
	public static int numOfclusters = 1;
	public static final String Table1 = "input";
	public static final String Table2 = "center";
	public static final String Family1 = "Area";
	public static final String Family2 = "Property";

	public static final String x1 = "X1";
	public static final String x2 = "X2";
	public static final String x3 = "X3";
	public static final String x4 = "X4";
	public static final String x5 = "X5";
	public static final String x6 = "X6";
	public static final String x7 = "X7";
	public static final String x8 = "X8";
	public static final String y1 = "Y1";
	public static final String y2 = "Y2";
	
	public static final String tempOutput="outputForDY";
//	public static final String file="dataset.txt";
	
	public static int getNumOfclusters() {
		return numOfclusters;
	}
	public static void setNumOfclusters(int numOfclusters) {
		globalNameSpace.numOfclusters = numOfclusters;
	}
	
	public static void putIntoHTable(Put HPut,String[] row){
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
	}


}
