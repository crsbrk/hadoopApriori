package com.chris.dataming.apriori;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;

import com.chris.dataming.alg.*;



public class AprioriKthFreqMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private ArrayList<HashSet<String>> daylist ;
	private ArrayList<HashSet<String>> nightList ;
	
	private ArrayList<HashSet<String>> newDayList;
	private ArrayList<HashSet<String>> newNightList;
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		System.out.println("in setup before context.getConfiguration");
		Configuration conf = context.getConfiguration();
		String pathOfItems = (String)conf.get("inputPathofItems");
		long k = (long)conf.getLong("kvalue",	0);
		
		System.out.println("in set:"+pathOfItems);
		
		//FileSystem fs = FileSystem.get(conf);
	
		daylist = Tools.readHdfsFileString(pathOfItems, conf, Tools.DAY);
		nightList = Tools.readHdfsFileString(pathOfItems, conf, Tools.NIGHT);
		
		Combination comb = new Combination();
		if (daylist != null && !daylist.isEmpty() ){
			Log.info("daylist.size"+daylist.size());
			newDayList = comb.getSetFrom(daylist,(int)k);
			}
		//剪枝
		//newDayList = comb.deleteUnFrequency(newDayList,daylist,(int)k);
		if (nightList != null && !nightList.isEmpty() ){
			newNightList = comb.getSetFrom(nightList,(int )k);
			}
		//剪枝
		//newNightList = comb.deleteUnFrequency(newNightList,nightList,(int)k);
	}

	void contextMapWrite(HashSet<String> ele,Context con, int t) 
			throws IOException, InterruptedException{
		String d = t==Tools.DAY?"day:":"night:";
		String k = "" ;
		
		for(String iteration :ele){
			k = k+iteration+":";
		}
		con.write(new Text(k), new Text(d+ 1));
	}
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line  = value.toString();
		String tokens[]  =  line.split("\\|",0);
		
//		for(String s :tokens){
//			System.out.println(s);
//		}
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
			dataTime= Integer.parseInt(tokens[9].substring(6,8));}
		else{
			System.out.println("~~~~Kth~~~~~time string error!!!@@@@##$$$$$$$$$"+123);
			dataTime = 0;
		}
		String msisdn = tokens[12];
		String contentChargeID = (tokens[15].isEmpty())?"0":tokens[15];
		
		if (!contentChargeID.equals("0")){
		
			 	String contentIDs[] =contentChargeID.split(":", 0);
			 	HashSet<String> contIDs = new HashSet<String>();
				for(String myid : contentIDs){
					System.out.println(myid);
					String id[] = myid.split("_");
					if (!Tools.itemInArray(id[0],Tools.DEFAULT_CHARGE_ID)){
						contIDs.add(id[0]);
					}
					
				}
				if(!contIDs.isEmpty()){
					HashSet<String> ele ;
					if(dataTime >= 9 && dataTime <=18 && newDayList != null){
						for(int i=0; i< newDayList.size();i++){
							ele = newDayList.get(i);
							if (contIDs.size() >=ele.size() &&
									contIDs.containsAll(ele)){
								contextMapWrite(ele,context,Tools.DAY);
							}
						}
						
					}else{//night
						if (newNightList!=null) {
							for (int i = 0; i < newNightList.size(); i++) {
								ele = newNightList.get(i);
								if (contIDs.size() >=ele.size() && 
										contIDs.containsAll(ele)) {//contIDs.size >= ele.size
									contextMapWrite(ele, context, Tools.NIGHT);
								}
							}
							
						}
					}
					
					
			
				}	
				}
		
		}
		
		
	}
	

}
