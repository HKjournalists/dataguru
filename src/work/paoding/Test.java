package work.paoding;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

public class Test {
	public static void main(String[] args) {
		String lineString = "亚洲：中华人名共和国";
		
		PaodingAnalyzer analyzer = new PaodingAnalyzer();
		StringReader srReader = new StringReader(lineString);
		TokenStream tsStream = analyzer.tokenStream("", srReader);
		try{
			while(tsStream.incrementToken()){
				CharTermAttribute taAttribute = tsStream.getAttribute(CharTermAttribute.class);
				System.out.println(taAttribute.toString());
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
