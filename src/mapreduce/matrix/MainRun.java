package mapreduce.matrix;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

import sun.java2d.pipe.SpanClipRenderer;

public class MainRun {
	public static final String HDFS="hdfs://192.168.142.133:9000";
	public static final Pattern DELIMITER=Pattern.compile("[\t,]");
	
	public static void main(String[] args) {
		matrixMultiply();
		//sparseMatrixMultiply();
	}
	
	public static void matrixMultiply(){
		Map<String, String > path = new HashMap<String,String>();
		path.put("m1", "datafile/matrix/m1.csv");//本地的数据文件
		path.put("m2", "datafile/matrix/m2.csv");
		path.put("input", HDFS+"/exp/data/matrix");//HDFS的目录
		path.put("input1", HDFS+"/exp/data/matrix/m1");
		path.put("input2", HDFS+"/exp/data/matrix/m2");
		path.put("output", HDFS+"/exp/result/matrix");
		
		try {
			MatrixMultiply.run(path);//启动程序
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	public static void sparseMatrixMultiply(){
		Map<String, String> path = new HashMap<String,String>();
		path.put("m1", "datafile/matrix/sm1.csv");
		path.put("m2", "datafile/matrix/sm2.csv");
		path.put("input", HDFS+"/exp/data/matrix");
		path.put("input1", HDFS+"/exp/data/matrix/m1");
		path.put("input2", HDFS+"/exp/data/matrix/m2");
		path.put("output", HDFS+"/exp/result/spmatrix");
		
		try {
			SparseMatrixMultiply.run(path);//启动程序
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	public static JobConf config(){//Hadoop集群的远程配置信息
		JobConf conf = new JobConf(MainRun.class);
		conf.setJobName("MatrixMultiply");
//		conf.addResource("classpath:/hadoop/core-site.xml");
//		conf.addResource("classpath:/hadoop/hdfs-site.xml");
//		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
	
}
