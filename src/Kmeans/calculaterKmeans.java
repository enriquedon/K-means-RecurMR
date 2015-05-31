package Kmeans;

import java.util.ArrayList;
import java.util.List;

import Program.Program;
import Program.globalNameSpace;

public class calculaterKmeans {
	// List<List<Double>> centers = new ArrayList<List<Double>>();
	// static List<Double> curData = new ArrayList<Double>();

	public int calkmeans(List<String> centersInString, String dataString) {

		double[][] centers = new double[numOfclusters][numOfColumn];
		double[] curData = new double[numOfColumn];
		curData = parseData(dataString);
		centers = parseCenters(centersInString);

		return chooseTheClosetCenter(centers, curData);
	}

	private int chooseTheClosetCenter(double[][] centers,
			double[] curData) {
		double[] curCenter = new double[numOfColumn];
		int numOfClueter = 0;
		double distance=Double.MAX_VALUE;
		for (int i  = 0; i < centers.length; i++) {
			double tempsum=0;
			curCenter=centers[i];
			for (int j = 0; j < numOfColumn; j++) {
				tempsum+=Math.pow(curData[j]-curCenter[j], 2);
			}
			if(tempsum<distance){
				distance=tempsum;
				numOfClueter=i;
			}
		}

		return numOfClueter;
	}

	private double[][] parseCenters(List<String> centersInString) {
		if (centersInString.size() != numOfclusters) {
			System.err.println("centersInString.size(): "+centersInString.size()+" !=numOfclusters:"+" numOfclusters");
		}
		double[][] centers = new double[numOfclusters][numOfColumn];
		for (int i = 0; i < numOfclusters; i++) {
			centers[i] = parseData(centersInString.get(i));
		}
		return centers;
	}

	private double[] parseData(String dataString) {
		String[] curDataInString = new String[numOfColumn];
		double[] curData = new double[numOfColumn];
		curDataInString = dataString.split("	");
		for (int i = 0; i < numOfColumn; i++) {
			curData[i] = Double.parseDouble(curDataInString[i]);
		}
		return curData;
	}
	
	public static final int numOfColumn = globalNameSpace.numOfColumn;
	public static int numOfclusters = globalNameSpace.numOfclusters;

}
