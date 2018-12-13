package com.chris.dataming.alg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Tools {
	
	

    public static String DEFAULT_CHARGE_ID[]={
    	"1013200201",//wap default
    	"1013200203",//wap default
    	"16000001",//wap default
    	"16000011",//wap default
    	"1700000001",//net default
    	"1013200101"//wap1x默认计费	
    	
    };
    
    
   // public static long MinSupport = 2;
    public static int DAY = 1;
    public static int NIGHT = 2;
    public static int CDRITEMS=20;//numbers of a record cdr pieces
    
    public static enum FileRecorder{  
        ErrorRecorder,  
        TotalRecorder  
    }  
    
    public static long readFileByChars(String fileName) {
        File file = new File(fileName);
        long tempchar = 0;
        Reader reader = null;
        try {
            System.out.println("以字符为单位读取文件内容，一次读一个字节：");
            // 一次读一个字符
            reader = new InputStreamReader(new FileInputStream(file));
            
            while ((tempchar = reader.read()) != -1) {
                // 对于windows下，\r\n这两个字符在一起时，表示一个换行。
                // 但如果这两个字符分开显示时，会换两次行。
                // 因此，屏蔽掉\r，或者屏蔽\n。否则，将会多出很多空行。
                if (((char) tempchar) != '\r') {
                    System.out.print((char) tempchar);
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempchar;
}
    
    public static long readFileByLine(String pathname) throws IOException{
       // try { // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw  
        	  
            /* 读入TXT文件 */  
            //String pathname = "D:\\twitter\\13_9_6\\dataset\\en\\input.txt"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径  
            File filename = new File(pathname); // 要读取以上路径的input。txt文件  
            InputStreamReader reader = new InputStreamReader(  
                    new FileInputStream(filename)); // 建立一个输入流对象reader  
            BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
            String line = "";  
            line = br.readLine();
            String lines = line.substring(1);
            long trans_num = Integer.parseInt(lines);
//            while (line != null) {  
//                line = br.readLine(); // 一次读入一行数据  
//            }  
    return trans_num;
    
}//end of readFileByLine
    
    
    public static long readHdfsFile(String path,FileSystem fs, Configuration conf){
    	String s = "";
    	String result = "";
    	  try {
              //Configuration conf = new Configuration();
              //FileSystem fs = FileSystem.get(new URI("hdfs://cluster1"), conf);
              Path file = new Path(path+"part-r-00000");//new Path("/cdr/items/part-r-00000");
              FSDataInputStream getIt = fs.open(file);
              BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
              
              while ((s = d.readLine()) != null) {
            	  System.out.println("in readHdfsFIle");
                  System.out.println(s);
                  result = s.trim();
              }
             
              
              d.close();
              fs.close();
          } catch (Exception e) {
              e.printStackTrace();
          }
    	  
          //result = s;//.substring("linesCount".length());
    	  System.out.println("result->"+result);
    	  return Long.parseLong(result);
    	  
      }
    /**
     * judege whether specific file in path is null or 0 byte
     * */
    public static boolean inPathFillNull(String path,Configuration conf,
    		int flg) throws IOException{
    	
		Path p = new Path(path);
		Path dayPath = new Path(path+Path.SEPARATOR+"part-r-00001");
		Path nightPath = new Path(path +Path.SEPARATOR+"part-r-00002");
		Path des = flg==Tools.DAY?dayPath:nightPath;
		
		FileSystem 	
		hdfs = p.getFileSystem(conf);
		
		if (!hdfs.isDirectory(p)) {
			return true;
		}else{
			  
		       
		        FileStatus status[] = hdfs.globStatus(p);  
		        if (status==null || status.length==0) {  
		        return true;
		          
		        }
		        
		        if (!hdfs.exists(dayPath) || 0==hdfs.globStatus(des).length){
		        	
		        	return true;
		        }
		    
		}
    	return false;
    }
    
    public static ArrayList<HashSet<String>> readHdfsFileString(String path, 
    		Configuration conf,int flag) throws IllegalArgumentException, IOException{
    	
    	if (inPathFillNull( path, conf,flag)) 
    		return null;
    	
    	String s = "";
    	String result = "";
    	Path file ;
    	//conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    	FileSystem fs =new Path(path).getFileSystem(conf);
    	ArrayList<HashSet<String>> list = new ArrayList<HashSet<String>>();
    	HashSet<String> item ;//= new HashSet<String>();
    	
    	  try {
              //Configuration conf = new Configuration();
              //FileSystem fs = FileSystem.get(new URI("hdfs://cluster1"), conf);
              
    		  if (flag==DAY)
    			  file = new Path(path+"part-r-00001");//new Path("/cdr/items/part-r-00000");
    		  else
    			  file = new Path(path+"part-r-00002");
    		  FSDataInputStream getIt = fs.open(file);
              BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
              String id;
              while ((s = d.readLine()) != null) {
            	  item = new HashSet<String>();
            	  
            	  System.out.println("in readHdfsFIle");
                  System.out.println(s);
                  
                  if(s.length()>0){
                	  result = s.trim().split("\\t")[0];
                	  String conId[] = result.split("\\:");
                	  
                	  for (int i=0; i<conId.length;i++){
                		  item.add(conId[i]);}
                  }
                  list.add(item);
                 
                  
              }
             
              
              d.close();
              //fs.close();
          } catch (Exception e) {
              e.printStackTrace();
          }
    	  
          //result = s;//.substring("linesCount".length());
    	  //System.out.println("result->"+result);
    	  return list;
    	  
      }
    
    
    
	/**
	 * check whether string a in string b[]
	 * */
	public static boolean  itemInArray(String a, String b[]){
		boolean flag = false;
		for (String item : b){
			if (item.equals(a)){
				flag = true;
				break;
			}
			
		}
		return flag;
	}
	

	
	


}

