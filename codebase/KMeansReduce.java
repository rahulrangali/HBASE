package uci.eecs.dstd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class KMeansReduce extends TableReducer<Text, Text,ImmutableBytesWritable> {

	public static enum counter {
		CONVERGECOUNTER
	}

	List<String> finalListAvg = new ArrayList<String>();
	double errorSum = 0.0;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		ArrayList<ArrayList<Double>> newAverages = new ArrayList<ArrayList<Double>>();
		ArrayList<Double> temp;
		for (Text value : values) {
			String rowValue = value.toString();
			String[] splitValues = rowValue.split("\\s+");
			temp = new ArrayList<Double>();
			for (String splitValue : splitValues) {
				temp.add(Double.parseDouble(splitValue));
			}
			newAverages.add(temp);
		}
		ArrayList<Double> tempList1 = newAverages.get(0);
		ArrayList<Double> finalAverages = new ArrayList<Double>();

		for (int i = 0; i < tempList1.size(); i++) {
			double sum = 0;
			for (int j = 0; j < newAverages.size(); j++) {
				ArrayList<Double> tempList = newAverages.get(j);
				sum += tempList.get(i);
			}
			sum = sum / newAverages.size();
			finalAverages.add(sum);
		}
		String finalAverage = "";
		for (int i = 0; i < finalAverages.size(); i++) {
			finalAverage += finalAverages.get(i) + " ";
		}
		finalListAvg.add(finalAverage);
		String keyString = key.toString();

		String[] keyStringSplit = keyString.split("\\s+");
		String[] finalAverageSplit = finalAverage.split("\\s+");
		for (int i = 0; i < keyStringSplit.length; i++) {
			Double dKeyStringSplit = Double.parseDouble(keyStringSplit[i]);
			Double dfinalAverageSplit = Double
					.parseDouble(finalAverageSplit[i]);
			errorSum += Math.abs(dKeyStringSplit - dfinalAverageSplit);
		}

		if (errorSum > 0.1)
			context.getCounter(counter.CONVERGECOUNTER).increment(1);
		//System.out.println("***********In KMeans Reduce***************** ");
	}


	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
			//System.out.println("Value= "+ finalListAvg.size());
		for (int i = 0; i < finalListAvg.size(); i++) {
			
			//System.out.println("******finalListAvg====="+finalListAvg);
			
			
			String newValues = finalListAvg.get(i);
			String[] newValue2 = newValues.split(" ");
			//for(int z=0;z<newValue2.length;z++)
				//System.out.println(newValue2[z]);
			Put put = new Put(Bytes.toBytes("Center"+(i+1)));	
			int j=1;
			for(String stringValues : newValue2)
			{
			
			put.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X"+j),Bytes.toBytes(stringValues));
			j++;
						
			}
			context.write(null,put);
			
			
			//context.write(new Text(finalListAvg.get(i)), new Text(""));
			
			System.out.println("*******Exiting KMeans Reduce*********88");

		}
	}

}
