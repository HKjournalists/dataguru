package mapreduce.matrix;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDAO {
	private static final String HDFS = "hdfs://192.168.142.133:9000";
	private String hdfsPath;
	private Configuration conf;
	
	public HdfsDAO(Configuration conf){
		this(HDFS,conf);
	}
	
	public HdfsDAO(String hdfs,Configuration conf){
		this.hdfsPath = hdfs;
		this.conf = conf;
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf = config();
		HdfsDAO hdfs  = new HdfsDAO(conf);
		hdfs.rename("/user/hdfs/pagerank/tmp3","/user/hdfs/pagerank/tmp4");
	}
	
	public static JobConf config(){
		JobConf conf = new JobConf(HdfsDAO.class);
		conf.setJobName("HdfsDAO");
		conf.addResource("classpatt:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
	
	//创建文件夹
	public void mkdirs(String folder) throws IOException{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create:" + folder);
		}
		fs.close();
	}
	
	//删除文件夹
	public void rmr(String folder) throws IOException{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.deleteOnExit(path);
		System.out.println("Delete:" + folder);
		fs.close();
	}
	
	//重命名
	public void rename(String src,String dst) throws IOException{
		Path name1 = new Path(src);
		Path name2 = new Path(dst);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.rename(name1, name2);
		System.out.println("Rename:from" + src + "to:" + dst);
		fs.close();
	}
	
	//列出文件夹内文件
	public void ls(String folder) throws IOException{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls:" + folder);
		System.out.println("=====================================");
		for(FileStatus f : list){
			System.out.println("name:" + f.getPath().toString() + "folder:" + f.isDir() + "size:" + f.getLen());
		}
		System.out.println("=====================================");
		fs.close();
	}
	
	//创建文件
	public void createFile(String file,String content) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff,0,buff.length);
			System.out.println("create:" + file);
		} finally {
			// TODO: handle exception
			if(os!= null)
				os.close();
		}
	}
	
	//从本地拷贝文件到hdfs
	public void copyFile(String local,String remote) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from:" + local + "to:" + remote);
		fs.close();
	}
	
	//从HDFS下载文件到本地
	public void download(String remote,String local) throws IOException{
		Path path = new Path(remote);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyToLocalFile(path,new Path(local));
		System.out.println("download: from" + remote + "to:" + local);
		fs.close();
	}
	
	//查看文件内容
	public void cat(String remoteFile) throws IOException{
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FSDataInputStream fsdisDataInputStream = null;
		System.out.println("cat" + remoteFile);
		try{
			fsdisDataInputStream = fs.open(path);
			IOUtils.copyBytes(fsdisDataInputStream, System.out, 4096,false);
		}finally{
			IOUtils.closeStream(fsdisDataInputStream);
			fs.close();
		}
	}
	
	public void location() throws IOException{
		
	}
	
	
}
