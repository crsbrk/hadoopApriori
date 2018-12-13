package com.chris.dataming.apriori;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AprioriReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable< Text> values, Context context) 
			throws IOException, InterruptedException {
		int maxScore = Integer.MIN_VALUE;
		String name = " ";
		String age = " ";
		String gender = " ";
		int score = 0;
		for (Text val : values) {
			String[] valTokens = val.toString().split("\\t");
			score = Integer.parseInt(valTokens[2]);
			if (score > maxScore) {
				name = valTokens[0];
				age = valTokens[1];
				gender = key.toString();
				maxScore = score;
			}
		}
		context.write(new Text(name), new Text("age- " + age + "\t"+ gender + "\tscore-" + maxScore));
	} 
}
