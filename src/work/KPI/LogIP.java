package work.KPI;
//package work;
///*
// * ͳ��IP��Դ
// */
//
//import java.io.IOException;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Set;
//import java.util.StringTokenizer;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.server.namenode.status_jsp;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//
//public class LogIP {
//
//    public static class KPIIPMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
//        private Text ips = new Text();
//        private final static IntWritable one = new IntWritable(1);
// 
//        public void map(Object key, Text value, Text ipText,IntWritable intWritable,Reporter reporter) throws IOException,InterruptedException {
//        	NginxLog nginxLog = new NginxLog();
//        	nginxLog = NginxLog.parse(value.toString());
//        	ips.set(nginxLog.getIp());
//        	ipText=ips;
//        	intWritable=one;
//        }
//        
//        /*
//         *  
//    ublic static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
//    
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//      
//    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//      StringTokenizer itr = new StringTokenizer(value.toString());
//      while (itr.hasMoreTokens()) {
//        word.set(itr.nextToken());
//        context.write(word, one);
//      }
//    }
//  }
//         */
//    }
//
//    public static class KPIIPReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterator<IntWritable> values,Text ipText,IntWritable intWritable, Reporter reporter) throws IOException,InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//              sum += val.get();
//            }
//            result.set(sum);
//            ipText = key;
//            intWritable = result;
//        }
//    }
//    /*
//    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//
//    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//      int sum = 0;
//      for (IntWritable val : values) {
//        sum += val.get();
//      }
//      result.set(sum);
//      context.write(key, result);
//    }
//  }
//     */
//
//    public static void main(String[] args) throws Exception {
//        String input = "hdfs://192.168.142.128:9000/home/grid/log_kpi/";
//        String output = "hdfs://192.168.142.128:9000/home/grid/weblog_ip/";
//
//        JobConf conf = new JobConf(LogIP.class);
//        conf.setJobName("LogIP");
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
//        
//        conf.setMapOutputKeyClass(Text.class);
//        conf.setMapOutputValueClass(Text.class);
//        
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(Text.class);
//        
//        conf.setMapperClass(KPIIPMapper.class);
//        conf.setCombinerClass(KPIIPReducer.class);
//        conf.setReducerClass(KPIIPReducer.class);
//
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//
//        FileInputFormat.setInputPaths(conf, new Path(input));
//        FileOutputFormat.setOutputPath(conf, new Path(output));
//
//        JobClient.runJob(conf);
//        System.exit(0);
//    }
//
//}
//
//
