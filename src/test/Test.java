package test;

/**  
 * Hadoop����γ�ģ�����
 * ��д�ߣ�James
 */  

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
 
/**  
 * ��Reducer�汾
 */  
public class Test extends Configured implements Tool {	
	
	/**  
	 * ������
	 * ���ڼ��������쳣����
	 */  
	enum Counter 
	{
		LINESKIP,	//�������
	}
	
	/**  
	 * MAP����
	 */  
	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
		{
			String line = value.toString();				//��ȡԴ����
			
			try
			{
				//���ݴ���
				String [] lineSplit = line.split(" ");
				String anum = lineSplit[0];
				String bnum = lineSplit[1];
				
				context.write( new Text(bnum), new Text(anum) );	//���
			}
			catch ( java.lang.ArrayIndexOutOfBoundsException e )
			{
				context.getCounter(Counter.LINESKIP).increment(1);	//�����������+1
				return;
			}
		}
	}

	/**  
	 * REDUCE����
	 */ 
	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			String valueString;
			String out = "";
			
			for ( Text value : values )
			{
				valueString = value.toString();
				out += valueString + "|";
			}
			
			context.write( key, new Text(out) );
		}
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();

		Job job = new Job(conf, "Test_2");								//������
		job.setJarByClass(Test.class);								//ָ��Class
		
		FileInputFormat.addInputPath( job, new Path(args[0]) );			//����·��
		FileOutputFormat.setOutputPath( job, new Path(args[1]) );		//���·��
		
		job.setMapperClass( Map.class );								//��������Map����ΪMap�������
		job.setReducerClass ( Reduce.class );							//��������Reduce����ΪReduce�������
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass( Text.class );							//ָ�������KEY�ĸ�ʽ
		job.setOutputValueClass( Text.class );							//ָ�������VALUE�ĸ�ʽ
		
		job.waitForCompletion(true);
		
		//�������������
		System.out.println( "�������ƣ�" + job.getJobName() );
		System.out.println( "����ɹ���" + ( job.isSuccessful()?"��":"��" ) );
		System.out.println( "����������" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() );
		System.out.println( "���������" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
		System.out.println( "�������У�" + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

		return job.isSuccessful() ? 0 : 1;
	}
	
	/**  
	 * ����ϵͳ˵��
	 * ����MapReduce����
	 */  
	public static void main(String[] args) throws Exception 
	{
		
		//�жϲ��������Ƿ���ȷ
		//����޲�����������ʾ��������˵��
		if ( args.length != 2 )
		{
			System.err.println("");
			System.err.println("Usage: Test_2 < input path > < output path > ");
			System.err.println("Example: hadoop jar ~/Test_2.jar hdfs://localhost:9000/home/james/Test_2 hdfs://localhost:9000/home/james/output");
			System.err.println("Counter:");
			System.err.println("\t"+"LINESKIP"+"\t"+"Lines which are too short");
			System.exit(-1);
		}
		
		//��¼��ʼʱ��
		DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();
		
		//��������
		int res = ToolRunner.run(new Configuration(), new Test(), args);

		//��������ʱ
		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "����ʼ��" + formatter.format(start) );
		System.out.println( "���������" + formatter.format(end) );
		System.out.println( "�����ʱ��" + String.valueOf( time ) + " ����" ); 

        System.exit(res);
	}
}
