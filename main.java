package wordcount2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class main implements Serializable
{//class main
	static double profit_2=0.0225,profit_3=0.0275,profit_5=0.0275;
	static int years=10;
	static String output="D:\\jinrongdata\\ans.txt";
	static String steps_out="D:\\jinrongdata\\steps.txt";
	static BufferedWriter bw;
	static BufferedWriter sbw;
    public static void main(String args[]) throws IOException
    {// f main
    	SparkConf conf=new SparkConf();
    	conf.setAppName("wordcount");
    	conf.setMaster("local");
    	
    	//File outfile=new File(output);
    	FileOutputStream out=new FileOutputStream(output);
    	FileOutputStream sout=new FileOutputStream(steps_out);
    	OutputStreamWriter fs=new OutputStreamWriter(out);
    	OutputStreamWriter ss=new OutputStreamWriter(sout);
    	//FileOutputStream fs=new FileOutputStream(outfile);
    	//OutputStreamWriter os=new OutputStreamWriter(fs,"utf8");
    	 bw=new BufferedWriter(fs);
    	 sbw=new BufferedWriter(ss);
    	String str;
    	JavaSparkContext sc=new JavaSparkContext(conf);
    	sbw.write("line50:对案例群体进行读取;");
    	JavaRDD<String> lines=sc.textFile("D:\\jinrongdata\\case.txt");
    	JavaRDD<String> cases=lines.flatMap(new FlatMapFunction<String,String>(){//f 01
        
			@Override
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split("!"));
			}
    		
    	});//f 01
    	sbw.write("line61:对每一条案例进行计算产生结果;");
       JavaPairRDD<String, String> results=cases.mapToPair(new PairFunction<String,String,String>(){//f 02

		@Override
		public Tuple2<String,String> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			String anstr=handle(arg0);
			return new Tuple2<String,String>(arg0,anstr);
		}
    	   
       });//f 02	
       sbw.write("line72:将运算结果输出;");
       results.foreach(new VoidFunction<Tuple2<String,String>>(){ 
		@Override
		public void call(Tuple2<String, String> result) throws Exception {
			// TODO Auto-generated method stub
			System.out.println(result._1+":"+result._2);
			 bw.write(result._1+":"+result._2+",");
			//bw.write(result._1+":"+result._2+"\n");
		}});     
       bw.close();
       fs.close();
       sbw.close();
       ss.close();
   /*    
      JavaPairRDD<String,Integer> wordcount=results.reduceByKey(new Function2<Integer,Integer,Integer>(){

		public Integer call(Integer v1, Integer v2) throws Exception {
            return v1+v2;
        }
    	  
      }); 
       

      
      wordcount.foreach(new VoidFunction<Tuple2<String,Integer>>(){

		@Override
		public void call(Tuple2<String, Integer> arg0) throws Exception {
			// TODO Auto-generated method stub
			System.out.println(arg0._1+":"+arg0._2);
		}
    	  
      });
     */ 
    }// f main
    public static double plan_rate(int index,int length) 
    {//plan_rate
    	if(length==3) 
    	{//if1
    		if(index==0) {//if11
    			return profit_2;
    		         }else if(index==1){//if11 if12
    		    return profit_3;     
    		         }else{//if12 if13
    		    return profit_5;    	 
    		         }//if13
    	}else{//if1 if2
    		if(index==0) {//if21
    		   return profit_3;	
    		}else{//if21 if22
    		   return profit_5;	
    		}//if22
    	}//if2
    }//plan_rate
    public static int plan_period(int index,int length) 
    {//plan_period
    	if(length==3) {//if1
    		if(index==0) {//if12
    			return 24;
    		}else if(index==1){//if12 13
    			return 36;
    		}else {//if13 14 
    			return 60;
    		}//14
    	   }else{//if1 2
    		   if(index==0) {
    			   return 36;
    		   }else {
    			   return 60;
    		   }
    	   }//if2
    }//plan_period
    public static String handle(String planstr) 
    {//handle
    	double profits[]=new double[years*12];
    	String[] months = planstr.split(";");
    	  for(int i=0;i<months.length;i++)
    	   {//for1
    		  String plans[]=months[i].split(",");
    		  for(int j=0;j<plans.length;j++) 
    		  {//for2
    			  double money=Double.parseDouble(plans[j]);
    			  int target=i+plan_period(j,plans.length);
    			  double profit = money*plan_rate(j,plans.length);
    			  double total_money=money+profit;
    			  profits[target]=total_money;
    		  }//for2
    	   }//for1
    	  for(int t=24;t<(years-5)*12;t++) 
    	  {//for2.5
    		  int target = t+60;
    		  double profit=profits[t]*profit_5;
    		  double totalmoney=profits[t]+profit;
    		  profits[target]=totalmoney;
    	  }//for2.5
    	  String anstr="";
    	  for(int k=0;k<profits.length;k++) 
    	   {//for3
    		  if(k!=profits.length-1) {//if1
    			  anstr+=profits[k]+",";
    		    }else{//if1 if2
    		      anstr+=profits[k]+"";	
    		    }//if2
    	   }//for3
    	  return anstr;
    }//handle
}//class main
