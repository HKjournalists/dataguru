package test;
 
import java.net.URI; 
import java.sql.Timestamp; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileStatus; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
 
/** 
 * 
 * Description:这个类演示如何通过FileSystem的getFileStatus()方法来获得FileStatus对象 
 * 进而查询文件或者目录的元信息 
 *  
 * 我们这里做2个实验，依次是获取HDFS中的某文件的元信息，获取HDFS中某目录的元信息 
 * 
 * @author charles.wang 
 * @created May 26, 2012 1:43:01 PM 
 * 
 * Hadoop中的FileStatus类可以用来查看HDFS中文件或者目录的元信息，
 * 任意的文件或者目录都可以拿到对应的FileStatus
 * 
 * http://supercharles888.blog.51cto.com/609344/879011
 *  
 */ 
public class FileStatusTest { 
 
    /** 
     * @param args 
     */ 
    public static void main(String[] args) throws Exception { 
        // TODO Auto-generated method stub          
         
        //读取hadoop文件系统的配置 
        Configuration conf = new Configuration(); 
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user"); 
         
        //实验1:查看HDFS中某文件的元信息 
        System.out.println("实验1:查看HDFS中某文件的元信息"); 
        String fileUri = args[0]; 
        FileSystem fileFS = FileSystem.get(URI.create(fileUri) ,conf); 
        FileStatus fileStatus = fileFS.getFileStatus(new Path(fileUri)); 
        //获取这个文件的基本信息       
        if(fileStatus.isDir()==false){ 
            System.out.println("这是个文件"); 
        } 
        System.out.println("文件路径: "+fileStatus.getPath()); 
        System.out.println("文件长度: "+fileStatus.getLen()); 
        System.out.println("文件修改日期： "+new Timestamp (fileStatus.getModificationTime()).toString()); 
        System.out.println("文件上次访问日期： "+new Timestamp(fileStatus.getAccessTime()).toString()); 
        System.out.println("文件备份数： "+fileStatus.getReplication()); 
        System.out.println("文件的块大小： "+fileStatus.getBlockSize()); 
        System.out.println("文件所有者：  "+fileStatus.getOwner()); 
        System.out.println("文件所在的分组： "+fileStatus.getGroup()); 
        System.out.println("文件的 权限： "+fileStatus.getPermission().toString()); 
        System.out.println(); 
         
        //实验2:查看HDFS中某目录的元信息 
        System.out.println("实验2:查看HDFS中某目录的元信息"); 
        String dirUri = args[1]; 
        FileSystem dirFS = FileSystem.get(URI.create(dirUri) ,conf); 
        FileStatus dirStatus = dirFS.getFileStatus(new Path(dirUri)); 
        //获取这个目录的基本信息       
        if(dirStatus.isDir()==true){ 
            System.out.println("这是个目录"); 
        } 
        System.out.println("目录路径: "+dirStatus.getPath()); 
        System.out.println("目录长度: "+dirStatus.getLen()); 
        System.out.println("目录修改日期： "+new Timestamp (dirStatus.getModificationTime()).toString()); 
        System.out.println("目录上次访问日期： "+new Timestamp(dirStatus.getAccessTime()).toString()); 
        System.out.println("目录备份数： "+dirStatus.getReplication()); 
        System.out.println("目录的块大小： "+dirStatus.getBlockSize()); 
        System.out.println("目录所有者：  "+dirStatus.getOwner()); 
        System.out.println("目录所在的分组： "+dirStatus.getGroup()); 
        System.out.println("目录的 权限： "+dirStatus.getPermission().toString()); 
        System.out.println("这个目录下包含以下文件或目录："); 
        for(FileStatus fs : dirFS.listStatus(new Path(dirUri))){ 
            System.out.println(fs.getPath()); 
        } 
         
         
    } 
 
}
