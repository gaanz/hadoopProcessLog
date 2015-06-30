package garn.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
public class ProcessLogService {
	
	private static final Logger LOG = Logger.getLogger(ProcessLogService.class);	

	
	
	public static String DB_URL = "jdbc:oracle:thin:@10.252.240.245:1521:campaigndb";

	// Database credentials
	public static  String USER = "neweServ";
	public static String PASS = "neweServ";

	public static Connection conn = null;
	public static Statement stmt = null;
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Class.forName("oracle.jdbc.OracleDriver");

		java.util.Properties info = new java.util.Properties();
		info.put("user", USER);
		info.put("password", PASS);
		info.put("useUnicode", "true");
		info.put("characterEncoding", "UTF-8");

		conn = DriverManager.getConnection(DB_URL, info);
		conn.setAutoCommit(false);
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ProcessLogService");
		job.setJarByClass(ProcessLogService.class);
		job.setMapperClass(CountSerivceLogMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logservice"));
		job.setInputFormatClass(NLinesInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logservice"));
		
		
	    job.waitForCompletion(true);
	    
//	    job = Job.getInstance(conf, "ProcessLogIncoming");
//		job.setJarByClass(ProcessLogService.class);
//		job.setMapperClass(CountIncomingLogMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logincoming"));
//		job.setInputFormatClass(NLinesInputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logincoming"));
//			  
//	    job.waitForCompletion(true);
//	    
//	    job = Job.getInstance(conf, "ProcessLogDetail");
//		job.setJarByClass(ProcessLogService.class);
//		job.setMapperClass(CountDetailLogMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logdetail"));
//		job.setInputFormatClass(NLinesInputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logdetail"));
//			  
//	    job.waitForCompletion(true);
//	    
//	    job = Job.getInstance(conf, "ProcessLogSummary");
//  		job.setJarByClass(ProcessLogService.class);
//  		job.setMapperClass(CountSummaryLogMapper.class);
//  		job.setCombinerClass(IntSumReducer.class);
//  		job.setReducerClass(IntSumReducer.class);
//  		job.setOutputKeyClass(Text.class);
//  		job.setOutputValueClass(IntWritable.class);
//  		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logsummary"));
//  		job.setInputFormatClass(NLinesInputFormat.class);
//  		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logsummary"));
//  			  
//  	    job.waitForCompletion(true);
//  	    
//  	    job = Job.getInstance(conf, "ProcessLogEvent");
//		job.setJarByClass(ProcessLogService.class);
//		job.setMapperClass(CountEventLogMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logevent"));
//		job.setInputFormatClass(NLinesInputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logevent"));
//			  
//	    job.waitForCompletion(true);
	    
	    System.exit(0);
	}
	
	public static String ModifyInput(String input , String name){
		
		if(input == null)
			return null;
		
		if(input.trim().equals("") || input.trim().equals("null"))
			return null;
		String output = input.replace(name+"=", "");
		if(output.length()>4000){
			output = output.substring(0, 3999);
		}
		return output;	
	}
}