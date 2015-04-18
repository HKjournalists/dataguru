package mapreduce.text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TokenizeDriver {

	public static void main(String[] args) throws Exception {
		
		// set configuration
		Configuration conf = new Configuration();
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 4000000);    //max size of Split
		//conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 6000000); 
		//conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 100000);
		
		Job job = new Job(conf,"Tokenizer");
		job.setJarByClass(TokenizeDriver.class);

	    // specify input format
		job.setInputFormatClass(MyInputFormat.class);
		
        //  specify mapper
		job.setMapperClass(TokenizeMapper.class);
		
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// specify input and output DIRECTORIES 
		//第七周 电子类
/*		String inputString = "hdfs://192.168.142.133:9000/exp/data/digital/";
		String outputString = "hdfs://192.168.142.133:9000/exp/result/digital";*/
		//第八周 体育类
/*		String inputString = "hdfs://192.168.142.128:9000/exp/data/sport/";
		String outputString = "hdfs://192.168.142.128:9000/exp/result/sport";*/
		//用户阅读文章类别
		String inputString = "hdfs://192.168.142.128:9000/exp/data/user-sport/";
		String outputString = "hdfs://192.168.142.128:9000/exp/result/user-sport";
		Path inPath = new Path(inputString);
		Path outPath = new Path(outputString);
		try {                                            //  input path
			FileSystem fs = inPath.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(inPath);
			for(int i=0; i<stats.length; i++)
				FileInputFormat.addInputPath(job, stats[i].getPath());
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}			
        FileOutputFormat.setOutputPath(job,outPath);     //  output path

		// delete output directory
		try{
			FileSystem hdfs = outPath.getFileSystem(conf);
			if(hdfs.exists(outPath))//如果目录存在，则删除目录
				hdfs.delete(outPath);
			hdfs.close();
		} catch (Exception e){
			e.printStackTrace();
			return ;
		}
		
		//  run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

