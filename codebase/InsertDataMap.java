package uci.eecs.dstd;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InsertDataMap extends Mapper <LongWritable, Text, ImmutableBytesWritable, Put>{
	int i=0;
	@Override
	@SuppressWarnings("resource")
	public void map(LongWritable key, Text value, Context context) throws IOException{

		Configuration conf = context.getConfiguration();

		String table = "Input";

		String area = "Area";
		String property = "Property";
	    String[] column = value.toString().split("\\s+");

	   // System.out.println("*************Entered Insert Data Map**************");


	    HTable hTable = new HTable(conf, table);



			Put row = new Put(Bytes.toBytes("row"+i++));
	    	//Put row = new Put(Bytes.toBytes("row"+key));
	    	int col1=1;
		    int col = 1;

	    for(String columnVal : column){

	    	if(col ==1 || col==5 || col==6){

	    		row.add(Bytes.toBytes(area),Bytes.toBytes("X"+""+col),Bytes.toBytes(columnVal));
	    		col++;

	    	}
	    	else if(col ==2 || col==3 || col==4 || col==7 || col==8){

	    		row.add(Bytes.toBytes(property),Bytes.toBytes("X"+""+col),Bytes.toBytes(columnVal));
	    		col++;

	    	}
	    	else if(col ==9 || col==10){

	    		row.add(Bytes.toBytes(area),Bytes.toBytes("Y"+""+col1),Bytes.toBytes(columnVal));
	    		col++;
	    		col1++;

	    	}

	    }

	    hTable.put(row);


	    hTable.flushCommits();
	    }

}
