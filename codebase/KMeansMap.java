package uci.eecs.dstd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class KMeansMap extends TableMapper<Text, Text> {

	ArrayList<ArrayList<Double>> centroids = new ArrayList<ArrayList<Double>>();

	protected void setup(Context context) throws IOException,
	InterruptedException {

        //System.out.println("*************In Setup of KMeans Map**************");

		Configuration config = HBaseConfiguration.create();



		//config = context.getConfiguration();
		//String tablename = config.get("TableName");

		@SuppressWarnings("resource")
		HTable currentCenter = new HTable(config, "Center");
		Scan s = new Scan();
		//String area = "Area";
		//String property = "Property";
		ResultScanner scanner = currentCenter.getScanner(s);
		ArrayList<Double> temp;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			temp = new ArrayList<Double>();
			byte[] key = rr.getRow();
			//System.out.println("I am here");
			//System.out.println(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X1")))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X1")))))));
			//System.out.println("CHECK THIS VALUE "+Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X1")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X2")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X3")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X4")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X5")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X6")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X7")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X8")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X9")))))));
			temp.add(Double.parseDouble(Bytes.toString(Bytes.toBytes(Bytes.toString(rr.getValue(Bytes.toBytes("Centroid"),Bytes.toBytes("X10")))))));

			centroids.add(temp);
			//System.out.println(temp);

		}



		

}

	public void map(ImmutableBytesWritable row, Result value,
			Context context) throws IOException, InterruptedException {
		ArrayList<Double> doubleList = new ArrayList<Double>();

		System.out.println("*************In Map of Kmeans Map**************");

		for (int i = 1; i <= 10; i++) {
			if (i == 1 || i == 5 || i == 6) {
				Double i1 = Double.parseDouble(new String(value.getValue(
						Bytes.toBytes("Area"), Bytes.toBytes("X" + i))));
								doubleList.add(i1);
			} else if (i == 2 || i == 3 || i == 4 || i == 7 || i == 8) {
				Double i1 = Double
						.parseDouble(new String(value.getValue(
								Bytes.toBytes("Property"),
								Bytes.toBytes("X" + i))));
				doubleList.add(i1);
			} else if (i == 9) {
				Double i1 = Double.parseDouble(new String(value.getValue(
						Bytes.toBytes("Area"), Bytes.toBytes("Y1"))));
				doubleList.add(i1);
			} else if (i == 10) {
				Double i1 = Double.parseDouble(new String(value.getValue(
						Bytes.toBytes("Area"), Bytes.toBytes("Y2"))));
				doubleList.add(i1);
			}
		}
		double min = Double.MAX_VALUE;
		int bestCentroid = 0;

		for (int j = 0; j < centroids.size(); j++) {
			ArrayList<Double> tempArrayList = centroids.get(j);
			double sum = 0;
			for (int i = 0; i < 9; i++) {

				sum += Math.abs(tempArrayList.get(i) - doubleList.get(i));
			}

			if (sum < min) {
				min = sum;
				bestCentroid = j;
			}
		}

		ArrayList<Double> bestCentroidValues = centroids.get(bestCentroid);
		String outputKey = "";
		for (int i = 0; i < bestCentroidValues.size(); i++) {
			outputKey += bestCentroidValues.get(i) + " ";
		}

		String outputValue = "";
		for (int i = 0; i < doubleList.size(); i++) {
			outputValue += doubleList.get(i) + " ";
		}

		context.write(new Text(outputKey), new Text(outputValue));
	}
}
