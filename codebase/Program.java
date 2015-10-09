package uci.eecs.dstd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Program {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		final String tableName = "Input";
		final String input = args[0];
		final int numberOfClusters = Integer.parseInt(args[1]);



		

		System.out.println("*************In Main**************");
		// final String inputToKCentroids = args[4];

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		System.out.println("*************Creating Input Table**************");

		if (admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);

		}
		if (admin.tableExists("Center")){
			admin.disableTable("Center");
			admin.deleteTable("Center");

		}
		
		
		HTableDescriptor desc = new HTableDescriptor(tableName);

		conf.setStrings("TableName", tableName);
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("Area")));
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("Property")));
		admin.createTable(desc);
		System.out.println("***********Table Created*************");

		Job job1 = new Job(conf, "createTable");
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setJarByClass(Program.class);
		job1.setMapperClass(InsertDataMap.class);
		job1.setOutputKeyClass(ImmutableBytesWritable.class);
		job1.setOutputValueClass(Put.class);
		job1.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job1, new Path(input));
		

		job1.waitForCompletion(true);

		System.out.println("*************Data Inserted in Input Table**************");

		@SuppressWarnings("deprecation")
		HTableDescriptor desc1= new HTableDescriptor("Center");

		System.out.println("*************Creating Center Table**************");

		desc1.addFamily(new HColumnDescriptor(Bytes.toBytes("Centroid")));
		//desc1.addFamily(new HColumnDescriptor(Bytes.toBytes("Property")));
		admin.createTable(desc1);
		System.out.println("Table Center Created");
		String area = "Area";
		String property = "Property";

		Configuration newconf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HTable inputTable = new HTable(newconf,"Input");
		HTable centerTable = new HTable(newconf,"Center");
		String centernew = "Center";


		for (int i=1; i <= numberOfClusters ;i++ ){

			String rowkey = "row"+i;
			Put row = new Put(Bytes.toBytes("Center"+i));
			Get get= new Get(Bytes.toBytes(rowkey));
			Result result = inputTable.get(get);
			
			
			//System.out.println(row);

			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X1"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(area),Bytes.toBytes("X1")))));
		//	System.out.println(result.getValue(Bytes.toBytes(area),Bytes.toBytes("X1")));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X2"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(property),Bytes.toBytes("X2")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X3"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(property),Bytes.toBytes("X3")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X4"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(property),Bytes.toBytes("X4")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X5"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(area),Bytes.toBytes("X5")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X6"),Bytes.toBytes(Bytes.toString((result.getValue(Bytes.toBytes(area),Bytes.toBytes("X6"))))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X7"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(property),Bytes.toBytes("X7")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X8"),Bytes.toBytes(Bytes.toString((result.getValue(Bytes.toBytes(property),Bytes.toBytes("X8"))))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X9"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(area),Bytes.toBytes("Y1")))));
			row.add(Bytes.toBytes("Centroid"),Bytes.toBytes("X10"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes(area),Bytes.toBytes("Y2")))));
			
			centerTable.put(row);
			


		}

		System.out.println("*************Data Inserted in Center Table**************");

		long counter = 1;

		

		while (counter > 0) {


			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "KMeansCalculation");
			
			
			job2.setJarByClass(Program.class);

			Scan scan = new Scan();
			scan.setCaching(1);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob(
					tableName,
					scan,
					KMeansMap.class, 
					Text.class,
					Text.class,
					job2);
			
			TableMapReduceUtil.initTableReducerJob(centernew, KMeansReduce.class, job2);
			
			//job2.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);

			//FileOutputFormat.setOutputPath(job2, new Path(filename));
			job2.waitForCompletion(true);
			counter = job2.getCounters()
					.findCounter(KMeansReduce.counter.CONVERGECOUNTER)
					.getValue();

		}

		System.out.println("*************Exit In Main........ Program Completed**************");

	}
}
