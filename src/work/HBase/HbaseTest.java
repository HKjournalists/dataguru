package work.HBase;

/*
 * Java操作Hbase进行建表、删表以及对数据进行增删改查,条件查询
 * 代码出自：http://ganliang13.iteye.com/blog/1863406
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest  {
	
	   public static Configuration configuration;

	/**
	 * a.配置host, 例如:bfdbjc2 192.168.11.72 
	 * b.参考:hbase-site.xml: 
	 * <property> <name>hbase.zookeeper.quorum</name>
	 *			  <value>bfdbjc2,bfdbjc3,bfdbjc4</value>
	 * </property> 
	 * <property> <name>hbase.rootdir</name> 
	 * 			  <value>hdfs://bfdbjc1:12000/hbase</value>
	 * </property>
	 */
	    static {  
	        configuration = HBaseConfiguration.create(); 
		    //String ZOOKEEPER_QUORAM =  "zk-1:2181,zk-2:2181,zk-3:2181,zk-4:2181,zk-5:2181,zk-6:2181";

	        configuration.set("hbase.zookeeper.quorum","bfdbjc2:2181,bfdbjc3:2181,bfdbjc4:2181");  
	        configuration.set("hbase.rootdir", "hdfs://bfdbjc1:12000/hbase");  
	    }  
	    
	    
	    public static void main(String[] args) throws Exception {  
	       // createTable("gangliang13");   
	        //insertData("gangliang13");
	    	//deleteRow("gangliang13", "112");
	        //queryAllLimit("gangliang13",2); 
	    	//queryByRowKey("GidCross","0100020a000056c100004e374e267993");
	    	//queryByColumn("gangliang13","aaa1");
	    	//testLikeQuery("gangliang13","11");
	    	//queryByManyColumn("gangliang13");
	    	//queryAll("gangliang13");
	    	//System.out.println("input GidCross rawkey:"+args[0]);
	    	//queryByRowKey("GidCross", "0100020a000056c100004e374e267993");
	    	//testScanByTimeStamp("ganglia2",1367984937372L);

	    	deleteColumnData("t12","111","f1","",1371107067100L);
	    }  
	    
	    /**
	     * 如果存在要创建的表，那么先删除，再创建
	     * @param tableName
	     */
	    public static void createTable(String tableName) {  
	        System.out.println("start create table ......");  
	        try {  
	            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);  
	            if (hBaseAdmin.tableExists(tableName)) {  
	                hBaseAdmin.disableTable(tableName);  
	                hBaseAdmin.deleteTable(tableName);  
	                System.out.println(tableName + " is exist,detele....");  
	            }  
	            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
	            tableDescriptor.addFamily(new HColumnDescriptor("name"));  
	            tableDescriptor.addFamily(new HColumnDescriptor("age"));  
	            tableDescriptor.addFamily(new HColumnDescriptor("sex"));  
	            hBaseAdmin.createTable(tableDescriptor);  
	        } catch (MasterNotRunningException e) {  
	            e.printStackTrace();  
	        } catch (ZooKeeperConnectionException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	        System.out.println("end create table ......");  
	    }  
	    
	    /**
	     * 插入数据
	     * @param tableName
	     */
	    public static void insertData(String tableName) {  
	        System.out.println("start insert data ......");  
	        HTablePool pool = new HTablePool(configuration, 1000);  
	        HTableInterface table =  pool.getTable(tableName);  
	        Put put = new Put("111".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  
	        put.add("name".getBytes(), null, "aaa1".getBytes());// 本行数据的第一列  
	        put.add("age".getBytes(), null, "bbb1".getBytes());// 本行数据的第三列  
	        put.add("sex".getBytes(), null, "ccc1".getBytes());// 本行数据的第三列  
	        
	        Put put2 = new Put("222".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  
	        put2.add("name".getBytes(), null, "aaa2".getBytes());// 本行数据的第一列  
	        put2.add("name".getBytes(), "nickname".getBytes(), "aaabbbbbnick".getBytes());
	        put2.add("age".getBytes(), null, "bbb2".getBytes());// 本行数据的第三列  
	        put2.add("sex".getBytes(), null, "ccc2".getBytes());// 本行数据的第三列  
	        try {  
	            table.put(put);  
	            table.put(put2);
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	        System.out.println("end insert data ......");  
	    }  
	    
	    /**
	     * 
	     * @param tableName 取前面limit条
	     */
	    public static void queryAllLimit(String tableName,int limit) {  
	        HTablePool pool = new HTablePool(configuration, 1000); 
	        HTableInterface table = pool.getTable(tableName);
	        try {  
	            Scan scan = new Scan();  
	            scan.setCaching(1);  
	            Filter filter = new PageFilter(limit); 
	            scan.setFilter(filter);  
	            ResultScanner scanner = table.getScanner(scan);// 执行扫描查找 int num = 0;  
	            Iterator<Result> res = scanner.iterator();// 返回查询遍历器  
	            while (res.hasNext()) {  
	                Result result = res.next();  
	                table.setWriteBufferSize(1024*1024*1);
	                KeyValue[] kv = result.raw();
	                for (int i = 0; i < kv.length; i++) {
		                System.out.print(new String(kv[i].getRow()) + "  ");
	                    System.out.print(new String(kv[i].getFamily()) + ":");
	                    System.out.print(new String(kv[i].getQualifier()) + "  ");
	                    System.out.print(kv[i].getTimestamp() + "  ");
	                    System.out.println(new String(kv[i].getValue()));
	                }
	            }  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  
	    
	    public static void testScanByTimeStamp(String tablename,Long timestamp) throws IOException{
	            //Scan类常用方法说明
	            //指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns； 
	            // scan.addFamily(); 
	            // scan.addColumn();
	            // scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
	            // scan.setTimeRange(); //指定最大的时间戳和最小的时间戳，只有在此范围内的cell才能被获取.
	            // scan.setTimeStamp(); //指定时间戳
	            // scan.setFilter(); //指定Filter来过滤掉不需要的信息
	            // scan.setStartRow(); //指定开始的行。如果不调用，则从表头开始；
	            // scan.setStopRow(); //指定结束的行（不含此行）；
	            // scan.setBatch(); //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
	            
	            //过滤器
	            //1、FilterList代表一个过滤器列表
	            //FilterList.Operator.MUST_PASS_ALL -->and
	            //FilterList.Operator.MUST_PASS_ONE -->or
	            //eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	            //2、SingleColumnValueFilter
	            //3、ColumnPrefixFilter用于指定列名前缀值相等
	            //4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
	            //5、QualifierFilter是基于列名的过滤器。
	            //6、RowFilter
	            //7、RegexStringComparator是支持正则表达式的比较器。
	            //8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。
	    	 
	        	HTablePool pool = new HTablePool(configuration, 1000); 
		        HTableInterface table = pool.getTable(tablename);
	            Scan scan = new Scan();  

	            scan.setTimeStamp(timestamp);
	            //scan.setTimeRange(NumberUtils.toLong("1370336286283"), NumberUtils.toLong("1370336337163"));
	            //scan.setStartRow(Bytes.toBytes("quanzhou"));
	            //scan.setStopRow(Bytes.toBytes("xiamen"));
	            //scan.addFamily(Bytes.toBytes("info")); 
	            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));
	            
	            //查询列镞为info，列id值为1的记录
	            //方法一(单个查询)
	            // Filter filter = new SingleColumnValueFilter(
	            //         Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1")); 
	            // scan.setFilter(filter);
	            
	            //方法二(组合查询)
	            //FilterList filterList=new FilterList();
	            //Filter filter = new SingleColumnValueFilter(
	            //    Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
	            //filterList.addFilter(filter);
	            //scan.setFilter(filterList);
	            
	            ResultScanner rs = table.getScanner(scan);
	            
	            for (Result r : rs) {
	                for (KeyValue kv : r.raw()) {
	                    System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
	                            Bytes.toString(kv.getRow()), 
	                            Bytes.toString(kv.getFamily()), 
	                            Bytes.toString(kv.getQualifier()), 
	                            Bytes.toString(kv.getValue()),
	                            kv.getTimestamp()));
	                }
	            }
	            
	            rs.close();
	    }
	    
	    /**
	     * 对行key进行like查询
	     * @param table
	     * @param likeQuery
	     * @throws Exception
	     */
	    public static void testLikeQuery(String table,String likeQuery) throws Exception {  
	        Scan scan = new Scan();  
	        RegexStringComparator comp = new RegexStringComparator(likeQuery);  
	        RowFilter filter = new RowFilter(CompareOp.EQUAL, comp);  
	        scan.setFilter(filter);  
	        scan.setCaching(200);  
	        scan.setCacheBlocks(false);  
	        HTable hTable = new HTable(configuration, table);  
	        ResultScanner scanner = hTable.getScanner(scan);  
	        for (Result r : scanner) {  
	        	KeyValue[] kv = r.raw();
                for (int i = 0; i < kv.length; i++) {
	                System.out.print(new String(kv[i].getRow()) + "  ");
                    System.out.print(new String(kv[i].getFamily()) + ":");
                    System.out.print(new String(kv[i].getQualifier()) + "  ");
                    System.out.print(kv[i].getTimestamp() + "  ");
                    System.out.println(new String(kv[i].getValue()));
                } 
            }  
	    }  
	    
	    /**
	     * 查询表所有行
	     * @param tableName
	     */
	    public static void queryAll(String tableName) {  
	        HTablePool pool = new HTablePool(configuration, 1000); 
	        HTableInterface table = pool.getTable(tableName);
	        try {  
	            ResultScanner rs = table.getScanner(new Scan());  
	            for (Result r : rs) {  
	            	KeyValue[] kv = r.raw();
	                for (int i = 0; i < kv.length; i++) {
		                System.out.print(new String(kv[i].getRow()) + "  ");
	                    System.out.print(new String(kv[i].getFamily()) + ":");
	                    System.out.print(new String(kv[i].getQualifier()) + "  ");
	                    System.out.print(kv[i].getTimestamp() + "  ");
	                    System.out.println(new String(kv[i].getValue()));
	                } 
	            }  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  
	    
	    /**
	     * 根据行记录值删除
	     * @param tablename
	     * @param rowkey
	     */
	    public static void deleteRow(String tablename, String rowkey)  {  
	        try {  
	            HTable table = new HTable(configuration, tablename);  
	            List<Delete> list = new ArrayList<Delete>();  
	            Delete d1 = new Delete(rowkey.getBytes());  
	            list.add(d1);  
	            table.delete(list);  
	            System.out.println("删除行成功!");  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  
	    /**
	     * 根据时间戳删除记录,删除后再put 时间戳不能比原来值小,创建表时，指定版本记录数
	     * @param tblName
	     * @param rowKey
	     * @param family
	     * @param column
	     * @param timestamp
	     * @throws Exception
	     */
	    public static void deleteColumnData(String tblName,String rowKey,String family,String column,long timestamp) throws Exception{  
	        HTable htbl = new HTable(configuration,tblName);  
	        Delete dlt = new Delete(Bytes.toBytes(rowKey));  
	        dlt.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(column), timestamp);  
	        htbl.delete(dlt);  
	        htbl.flushCommits();  
	        htbl.close();  
	    }  
	    
	    /**
	     * 根据行记录索引
	     * @param tableName
	     * @param row
	     * @throws IOException
	     */
	    public static void queryByRowKey(String tableName,String row) throws IOException {  
	    	HTable table = new HTable(configuration, tableName); 
	    	System.err.println(table.getRegionLocation(row.getBytes()));
	        try {  
	            Get scan = new Get(row.getBytes());// 根据rowkey查询  
	            Result r = table.get(scan);  
	        	KeyValue[] kv = r.raw();
                for (int i = 0; i < kv.length; i++) {
	               // System.out.print(new String(kv[i].getRow()) + "  ");
                    //System.out.print(new String(kv[i].getFamily()) + ":");
                    System.out.print(new String(kv[i].getQualifier()) + "  ");
                   // System.out.print(kv[i].getTimestamp() + "  ");
                    System.out.println(new String(kv[i].getValue()));
                }
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  
	    
	    /** 
	     * 按列查询，查询多条记录 
	     * @param tableName 
	     */  
	    public static void queryByColumn(String tableName,String columnValue) {  
	        try {  
	            HTable table = new HTable(configuration,tableName);  
	            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("name"), null, CompareOp.EQUAL, Bytes.toBytes(columnValue)); // 当列column1的值为aaa时进行查询  
	            Scan s = new Scan();  
	            s.setFilter(filter);  
	            ResultScanner rs = table.getScanner(s);  
	            for (Result r : rs) {  
	            	KeyValue[] kv = r.raw();
	                for (int i = 0; i < kv.length; i++) {
		                System.out.print(new String(kv[i].getRow()) + "  ");
	                    System.out.print(new String(kv[i].getFamily()) + ":");
	                    System.out.print(new String(kv[i].getQualifier()) + "  ");
	                    System.out.print(kv[i].getTimestamp() + "  ");
	                    System.out.println(new String(kv[i].getValue()));
	                }
	            }  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }  
	    }  
	    
	    /** 
	     * 按多列查询，查询多条记录 
	     * @param tableName 
	     */  
	    public static void queryByManyColumn(String tableName) {  
	    	 try {  
	             HTable table =  new HTable(configuration,tableName); 
	             List<Filter> filters = new ArrayList<Filter>();  
	   
	             Filter filter1 = new SingleColumnValueFilter(Bytes  
	                     .toBytes("age"), null, CompareOp.EQUAL, Bytes.toBytes("bbb1"));  
	             filters.add(filter1);  
	   
	             Filter filter2 = new SingleColumnValueFilter(Bytes  
	                     .toBytes("name"), null, CompareOp.EQUAL, Bytes.toBytes("aaa1")); 
	             filters.add(filter2);  
	   
	             FilterList filterList1 = new FilterList(filters);  
	   
	             Scan scan = new Scan();  
	             scan.setFilter(filterList1);  
	             ResultScanner rs = table.getScanner(scan);  
	             for (Result r : rs) {  
            		KeyValue[] kv = r.raw();
                    for (int i = 0; i < kv.length; i++) {
    	                System.out.print(new String(kv[i].getRow()) + "  ");
                        System.out.print(new String(kv[i].getFamily()) + ":");
                        System.out.print(new String(kv[i].getQualifier()) + "  ");
                        System.out.print(kv[i].getTimestamp() + "  ");
                        System.out.println(new String(kv[i].getValue()));
                    }  
	             }  
	             rs.close();  
	   
	         } catch (Exception e) {  
	             e.printStackTrace();  
	         }  
	    }  
}

