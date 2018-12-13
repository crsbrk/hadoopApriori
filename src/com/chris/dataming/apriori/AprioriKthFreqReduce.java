package com.chris.dataming.apriori;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer.Context;

import com.chris.dataming.alg.Tools;


public class AprioriKthFreqReduce extends
		Reducer<Text, Text, Text, Text> {
	private long minSup = 2l;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		minSup = context.getConfiguration().getLong("minsupport", 0);
		super.setup(context);
	}

	//LongWritable result = new LongWritable();
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0l;
		for (Text val : values) {
			String v[] = val.toString().split(":");
			for(String s : v){
				System.out.println(s);
			}
			if (v[0].equals("night") || v[0].equals("day"))
				sum += Long.parseLong(v[1].trim());
			else
				sum += Long.parseLong(v[0].trim());
			
		}
		
		//result.set(sum);
		System.out.println(minSup+"minSup pass from configuration");
		if (sum >= minSup)//Tools.MinSupport)
			context.write(key, new Text(sum+""));
	}

}
