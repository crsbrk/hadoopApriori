package com.chris.dataming.apriori;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;

import com.chris.dataming.alg.Tools;

public class AprioriFirstFrequentMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String line  = value.toString();
			String tokens[]  =  line.split("\\|",0);
			
			for(String s :tokens){
				System.out.println(s);
			}
			if (tokens.length != Tools.CDRITEMS){
				context.getCounter(Tools.FileRecorder.ErrorRecorder).increment(1);
			}else{
			//160617235101-8613178800875-1700000001_913_487:1157632026_60_60:
			//System.out.println(tokens[9]+"-"+tokens[12]+"-<"+tokens[15]+">-"+tokens[9].substring(6,8));
			//System.out.println(tokens[15].isEmpty()?"token[15]==empty:true":"false");
			String workDay;
			String workNight;
			int dataTime ;
			if(tokens[9].length()>=8){
					dataTime = Integer.parseInt(tokens[9].substring(6,8));}
			else{
				System.out.println("~~~~~~~~~time string error!!!@@@@##$$$$$$$$$"+123);
				dataTime = 0;
			}
			String msisdn = tokens[12];
			String contentChargeID = (tokens[15].isEmpty())?"0":tokens[15];

			if (!contentChargeID.equals("0")){
			
				 	String contentIDs[] =contentChargeID.split(":", 0);
					for(String myid : contentIDs){
						System.out.println(myid);
						String id[] = myid.split("_");
						
						for(String s :id){
							System.out.println(s);
							
						}
						//System.out.println(id[0]);
						
						//need to exclude default charging content chargeID
						//default charging id is useless
						if (!Tools.itemInArray(id[0],Tools.DEFAULT_CHARGE_ID)){
							
							//key contentChargeID  ,value:day/night+one
							//9:00~18:00 day time   19:00-8:00 night time
							if(dataTime >= 9 && dataTime <=18 ){
								context.write(new Text(id[0]), new Text("day:"+ 1));
							}else{
								context.write(new Text(id[0]), new Text("night:"+ 1));
							}
						}
						
					
					}
			
			}
	}
	}
	

}
