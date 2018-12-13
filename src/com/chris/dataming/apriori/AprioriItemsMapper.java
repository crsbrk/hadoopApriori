package com.chris.dataming.apriori;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.chris.dataming.alg.Tools;

public class AprioriItemsMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	

	
	//private long count=1;
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		try {
	           //取得读取的行数
	        // count=key.get();
		
	         //正常逻辑
			//context.write(new Text(), new LongWritable());
	         if(value != null){  
	             /** 
	              * 把counter实现累加 
	              */  
	             context.getCounter(Tools.FileRecorder.TotalRecorder).increment(1);  
	           //  context.write(new Text("lineNumbers"), new LongWritable(1));
	         }  

	} catch (Exception e) {

		e.printStackTrace();
	}
		}
	//map 方法调用完后才调用的
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//map清理资源的操作
		//在reduce中把linescount取出来就行了
		//context.write(new Text(""), new LongWritable(count));
		//System.out.println(context.getCounter(Tools.FileRecorder.TotalRecorder).getValue());
		//context.write(new Text(""), new LongWritable(context.getCounter(Tools.FileRecorder.TotalRecorder).getValue()));
	}

}
