package com.chris.dataming.apriori;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AprioriPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		String[] dayNight = value.toString().split(":");
		//根据day and night to partion
		String dayOrNight = dayNight[0];
		//int ageInt = Integer.parseInt(age);	
		if (numReduceTasks == 0)
			return 0;	
			
		if (dayOrNight.equals("day")) {
		
			return 1 % numReduceTasks;
		}	
		else
			return 2 % numReduceTasks;	
	}
		
}
