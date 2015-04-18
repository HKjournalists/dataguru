package mapreduce.recommend_update;

/*
 * 对同线矩阵和评分矩阵进行转型
 */

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Step3 {
	public static class Step31_UserVectorSplitterMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();
		
		public void map(LongWritable key,Text values,OutputCollector<IntWritable, Text> output,Reporter reporter) throws IOException{
			String[] tokenStrings = Recommend.DELIMITER.split(values.toString());
			for(int i = 1;i<tokenStrings.length;i++){
				String[] vectorStrings= tokenStrings[i].split(":");
				int itemID = Integer.parseInt(vectorStrings[0]);
				String pref = vectorStrings[0];
				
				k.set(itemID);
				v.set(tokenStrings[0] + ":" + pref);
				output.collect(k, v);
			}
		}
	}
	
	public static void run1(Map<String, String> path) throws IOException{
		JobConf conf = Recommend.config();
		
		String inputString = path.get("Step3Input1");
		String outputString = path.get("Step3Output1");
		
		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS,conf);
		hdfs.rmr(outputString);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputString));
		FileOutputFormat.setOutputPath(conf, new Path(outputString));
		
		RunningJob job = JobClient.runJob(conf);
		while(!job.isComplete()){
			job.waitForCompletion();
		}
	}
	
	public static class Step32_CooccurrenceColumnWrapperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		private final static Text k = new Text();
		private final IntWritable  v = new IntWritable();
		
		public void map(LongWritable key,Text values,OutputCollector<Text, IntWritable> output,Reporter reporter) throws IOException{
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			k.set(tokens[0]);
			v.set(Integer.parseInt(tokens[1]));
			output.collect(k, v);
		}
	}
	
	public static void run2(Map<String, String> path) throws IOException{
		JobConf conf = Recommend.config();
		
        String input = path.get("Step3Input2");
        String output = path.get("Step3Output2");
        
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf,new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        
        RunningJob job = JobClient.runJob(conf);
        while(!job.isComplete()){
        	job.waitForCompletion();
        }
	}
}
