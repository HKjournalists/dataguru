package work.paoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class Paoding {
	public static void main(String[] args) {
		/* 读入TXT文件 */  
	    String pathname = "datafile/digital/mobile/mobile3"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径  
	    File filename = new File(pathname); // 要读取以上路径的input.txt文件  
	    InputStreamReader reader;
	    StringBuilder txtString = new StringBuilder();
	    
		try {
			reader = new InputStreamReader(new FileInputStream(filename));
			// 建立一个输入流对象reader  
		    BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
		    String line = ""; 
		    line = br.readLine();  
		    while (line != null) {  
		        line = br.readLine(); // 一次读入一行数据  
		        txtString.append(line);
		    }  
			System.out.println("原文件内容：" + txtString);	
		    
/*          //写入Txt文件   
            File writename = new File(".\\result\\en\\output.txt"); // 相对路径，如果没有则要建立一个新的output。txt文件  
            writename.createNewFile(); // 创建新文件  
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));  
            out.write("我会写入文件啦\r\n"); // \r\n即为换行  
            out.flush(); // 把缓存区内容压入文件  
            out.close(); // 最后记得关闭文件 
*/		    
		    PaodingAnalyzer analyzer = new PaodingAnalyzer();
			StringReader srReader = new StringReader(txtString.toString());
			TokenStream tsStream = analyzer.tokenStream("", srReader);
			while(tsStream.incrementToken()){
				CharTermAttribute taAttribute = tsStream.getAttribute(CharTermAttribute.class);
				System.out.println(taAttribute.toString());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

}
