package mapreduce.MaxTemperature;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Exercise_2 extends Configured implements Tool {	
	
	enum Counter 
	{
		LINESKIP,	
	}
	
 
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> 
	{
		
		private String name;
		public void setup ( Context context )
		{
			this.name = context.getConfiguration().get("name");				
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
		{
			String line = value.toString();		
			
			try
			{
				String [] lineSplit = line.split(" ");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
		
				Text out = new Text(this.name + ' ' + month + ' ' + time + ' ' + mac);		
				
				context.write( NullWritable.get(), out);
			}
			catch ( java.lang.ArrayIndexOutOfBoundsException e )
			{
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
		}
	}


	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();
		
		conf.set("name", args[2]);

		Job job = new Job(conf, "Exercise_2");							
		job.setJarByClass(Exercise_2.class);						
		
		FileInputFormat.addInputPath( job, new Path(args[0]) );		
		FileOutputFormat.setOutputPath( job, new Path(args[1]) );		
		
		job.setMapperClass( Map.class );							
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass( NullWritable.class );		
		job.setOutputValueClass( Text.class );						
		
		job.waitForCompletion(true);
		
		System.out.println( "任务名称" + job.getJobName() );
		System.out.println( "是否成功" + ( job.isSuccessful()?"成功":"失败" ) );
		System.out.println( "map输入" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() );
		System.out.println( "map输出" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
		System.out.println( "跳过的行" + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

		return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		if ( args.length != 3 )
		{
			System.err.println("");
			System.err.println("Usage: Test_1 < input path > < output path > < name >");
			System.err.println("Example: hadoop jar ~/Test_1.jar hdfs://localhost:9000/home/james/Test_1 hdfs://localhost:9000/home/james/output hadoop");
			System.err.println("Counter:");
			System.err.println("\t"+"LINESKIP"+"\t"+"Lines which are too short");
			System.exit(-1);
		}
		
		DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();
		
		int res = ToolRunner.run(new Configuration(), new Exercise_2(), args);

		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "开始时间" + formatter.format(start) );
		System.out.println( "结束时间" + formatter.format(end) );
		System.out.println( "用时" + String.valueOf( time ) + "时长" ); 

        System.exit(res);
	}
}
