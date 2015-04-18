package mapreduce.text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/*
 * 继承类CombineFileInputFormat
 重写方法isSplitable：判断所有文件都不可分割
 实现方法createRecordReader
     –返回一个CombineFileRecordReader对象
     –CombineFileRecordReader的构造函数中，指定RecordReader
 */


import org.apache.hadoop.util.LineReader;

public class MyInputFormat extends CombineFileInputFormat<Text, Text> {

	/**
	 *   make sure file will not be splitted
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	
	/**
	 *   specify record reader
	 *   返回的对象，负责从分片中读取key-value对
	 */
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		CombineFileRecordReader<Text, Text> recordReader = 	new CombineFileRecordReader<Text, Text>(
				(CombineFileSplit)split, context, MyRecordReader.class);
		return recordReader;
	}

}



