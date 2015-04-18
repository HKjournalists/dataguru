package work.call;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Call {

    public static class CallMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Text keyText = new Text();
        	Text valueText = new Text();
        	String keyString;
        	String [] callStrings = value.toString().split("\t");
        	double durtime = Double.parseDouble(callStrings[1]);
        	if (durtime < 0.3) {
        		keyString = callStrings[0] +"\t"+ callStrings[1];
        		keyText.set(keyString);
        		valueText.set(callStrings[2]);
        		context.write(keyText, valueText);
			}else{
				return;
			}
        }
    }

/*    public static class CallReducer extends Reducer<Text, Text, Text, Text> {
    	@Override
    	public void reduce(Text key,Iterator<Text> values,Context context) throws IOException, InterruptedException{
    		Text valueText = new Text();
    		while(values.hasNext()){
    			String valueString = values.next().toString();
    			double callOut = Double.parseDouble(valueString);
    			if (callOut > 0.6) {
    				valueText.set(valueString);
					context.write(key,valueText);
				}else {
					return;
				}
    		}
    	}
    }
*/
    
    public static class CallReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		Text valueText = new Text();
    		/*
    		while(values.hasNext()){
    			String valueString = values.next().toString();
    			double callOut = Double.parseDouble(valueString);
    			if (callOut > 0.6) {
    				valueText.set(valueString);
					context.write(key,valueText);
				}else {
					return;
				}
    		}
   			*/
    		for(Text value : values){
    			String valueString = value.toString();
    			double callOut = Double.parseDouble(valueString);
    			if (callOut > 0.6) {
    				valueText.set(valueString);
					context.write(key,valueText);
				}else {
					return;
				}
    		}
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();

        String input = "hdfs://192.168.142.133:9000/exp/data/call";
        String output = "hdfs://192.168.142.133:9000/exp/result/call";

        Job job = new Job(conf);
        job.setJarByClass(Call.class);
        job.setMapperClass(CallMapper.class);
        job.setReducerClass(CallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
