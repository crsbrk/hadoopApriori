package com.chris.dataming.apriori;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AprioriMap  extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String[] tokens = value.toString().split("<tab>");//使用分隔符< tab> 将数据解析成数组 tokens
		
		if (tokens != null ){
		String gender = tokens[2].toString();//性别
		String nameAgeScore = tokens[0] + "\t" + tokens[1] + "\t"+ tokens[3];//姓名+年龄+分数
		context.write(new Text(gender), new Text(nameAgeScore));//输出key=gender，value=name+age+score
		}
		}
}
