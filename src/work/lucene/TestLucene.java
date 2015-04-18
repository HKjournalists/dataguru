package work.lucene;

import java.io.File;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
//import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class TestLucene {
	private static String indexPath = "D:\\test";
	
	public static void createIndex() throws Exception{
		/*
		 * Index.ANALYZED 分词索引
		 * Index.No 不索引
		 * Index.NOT ANALYZED 索引不分词
		 * Store.No 不存储
		 * Store.YES 存储
		 * Store.compress 压缩存储
		 */
		Directory dir = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		Document doc = new Document();
		doc.add(new Field("name","zhangsan",Store.YES,Index.ANALYZED));
		doc.add(new Field("address","hangzhou",Store.YES,Index.ANALYZED));
		doc.add(new Field("sex","man",Store.YES,Index.ANALYZED));
		doc.add(new Field("introduce","I am a coder,my name is zhansan",Store.YES,Index.NO));
		IndexWriter indexWriter = new IndexWriter(dir, analyzer,MaxFieldLength.LIMITED);
		indexWriter.addDocument(doc);
		indexWriter.close();
	}
	
	public static void updateIndex() throws Exception{
		Directory dir = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		Document doc = new Document();
		doc.add(new Field("name","zhansan",Store.YES,Index.ANALYZED));
		doc.add(new Field("address","hangzhou",Store.YES,Index.ANALYZED));
		doc.add(new Field("sex","man",Store.YES,Index.ANALYZED));
		doc.add(new Field("introduce","I am a coder,my name is zhansan",Store.YES,Index.ANALYZED));
		IndexWriter indexWriter = new IndexWriter(dir, analyzer,MaxFieldLength.LIMITED);
		
		indexWriter.deleteDocuments(new Term("name","zhansan"));
		indexWriter.addDocument(doc);
		
		indexWriter.updateDocument(new Term("name","zhansan"),doc);
		indexWriter.close();
	}
	
	public static void searchIndex() throws Exception{
		String queryStr = "zhansan";
		String[] fields = {"name","introduce"};
		
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser queryParser = new MultiFieldQueryParser(Version.LUCENE_30, fields, analyzer);
		Query query = queryParser.parse(queryStr);
		
		Directory dirIndexPath = FSDirectory.open(new File(indexPath));
		IndexSearcher indexSearcher = new IndexSearcher(dirIndexPath);
		Filter filter = null;
		TopDocs topDocs = indexSearcher.search(query, filter,10000);
		
		System.out.println("hits:" + topDocs.totalHits);
		
		for(ScoreDoc scoreDoc : topDocs.scoreDocs){
			int docNum = scoreDoc.doc;
			Document doc = indexSearcher.doc(docNum);
			printDocumentInfo(doc);
		}
	}
	
	public static void complexQuery() throws Exception{
		//关键词查询
		Term term = new Term("name","zhansan");
		Query termQuery = new TermQuery(term);
		
		//范围查询
		NumericRangeQuery numericRangeQuery 
			= NumericRangeQuery.newIntRange("size", 2, 100, true, true);
		
		//通配符查询
		Term wildcardTerm = new Term("name","zhansa?");
		WildcardQuery wildcardQuery = new WildcardQuery(wildcardTerm);
		
		//短语查询
		PhraseQuery phraseQuery = new PhraseQuery();
		phraseQuery.add(new Term("content","dog"));
		phraseQuery.add(new Term("content","cat"));
		phraseQuery.setSlop(5);//单词之间的间隔
		
		//布尔查询
		PhraseQuery query1 = new  PhraseQuery();
		query1.add(new Term("content","dog"));
		query1.add(new Term("content","cat"));
		query1.setSlop(5);
		
		Term wildTerm = new Term("name","zhans?");
		WildcardQuery query2 = new WildcardQuery(wildTerm);
		
		BooleanQuery booleanQuery = new BooleanQuery();
		booleanQuery.add(query1,Occur.MUST);
		booleanQuery.add(query2,Occur.MUST);//条件必须出现
		
		//对不同field设置权重
		String queryStr = "dog cat";
		String[] fields = {"name","content","size"};
		Map<String, Float> weights = new HashMap<String, Float>();
		weights.put("name", 3.5f);
		weights.put("content", 0.2f);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser queryParser = new MultiFieldQueryParser(Version.LUCENE_30, fields,analyzer,weights);
		org.apache.lucene.search.Query query = queryParser.parse(queryStr);
	}
	
	public static void testSort() throws Exception{
		//按照某个field排序
		String queryStr = "lishi";
		String[] fields = {"name","address","size"};
		Sort sort = new Sort();
		SortField field = new SortField("size", SortField.INT,true);//第二个参数表示是否反转
		sort.setSort(field);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser queryParser = new MultiFieldQueryParser(Version.LUCENE_30, fields, analyzer);
		Query query = queryParser.parse(queryStr);
		Directory dirIndexPath = FSDirectory.open(new File(indexPath));
		IndexSearcher indexSearcher = new IndexSearcher(dirIndexPath);
		//IndexSearcher indexSearcher = new IndexSearcher(IndexPath);
		Filter filter = null;
		TopDocs topDocs = indexSearcher.search(query, 100);
		for(ScoreDoc scoreDoc:topDocs.scoreDocs){
			int docNum = scoreDoc.doc;
			Document doc = indexSearcher.doc(docNum);
			printDocumentInfo(doc);
		}
	}
	
	public static void testHightLight() throws Exception{
		String queryStr = "zhansan";
		String[] fields = {"name","introduce"};
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser queryParser = new MultiFieldQueryParser(Version.LUCENE_30, fields, analyzer);
		Query query = queryParser.parse(queryStr);
		
		Formatter formater = new SimpleHTMLFormatter("<font color='red'>","</font>");
		Scorer scorer = new QueryScorer(query);
		Highlighter highLight = new Highlighter(formater,scorer);
		Fragmenter fragmenter = new SimpleFragmenter(20);
		highLight.setTextFragmenter(fragmenter);
		
		Directory dirIndexPath = FSDirectory.open(new File(indexPath));
		IndexSearcher indexSearcher = new IndexSearcher(dirIndexPath);
		Filter filter = null;
		TopDocs topDocs = indexSearcher.search(query, filter,100);
		
		for(ScoreDoc scoreDoc:topDocs.scoreDocs){
			int docNum = scoreDoc.doc;
			Document doc = indexSearcher.doc(docNum);
			String hc = highLight.getBestFragment(analyzer, "introduce",doc.get("introduce"));
			if(hc == null){
				String introduce = doc.get("introduce");
				int endIndex = Math.min(50, introduce.length());
				hc = introduce.substring(0,endIndex);
			}
			doc.getField("introduce").setValue(hc);
			printDocumentInfo(doc);
		}
	}
	
	public static void testAnalyzer() throws Exception{
		String enContent = "I am a chinese man,I love my country";
		String zhContent = "我爱北京天安门";
		
		Analyzer standAnalyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);//使用最新的标准分词器，可以单字分解中文
		Analyzer sumpleAnalyzer = new SimpleAnalyzer();//不能分解中文
		
		Analyzer cjkAnalyzer = new CJKAnalyzer(Version.LUCENE_30);//二分法分词中文分词 中日韩文分词
		Analyzer chineseAnalyzer = new ChineseAnalyzer();//中文分词，单个分解
		
		//Analyzer mmAnalyzer = new MMAnalyzer();//词库分词采用极易分词
		Analyzer ikAnalyzer = new IKAnalyzer();//IK分词
		Analyzer paodingAnalyzer = new PaodingAnalyzer();//庖丁分词
		
		analyze(ikAnalyzer, zhContent);
	}
	
	public static void analyze(Analyzer analyzer,String text) throws Exception{
		System.out.println("\n分词器："+ analyzer.getClass());
		TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(text));
		/*
		Token token = tokenStream.next();
		while(token != null){
			System.out.println(token);
			token = tokenStream.next();
		}
		*/
		//boolean token = tokenStream.incrementToken();
	    CharTermAttribute termAtt = (CharTermAttribute) tokenStream.getAttribute(CharTermAttribute.class);
		while(tokenStream.incrementToken()){
			//System.out.println(token);
	        String token = new String(termAtt.buffer(),0,termAtt.length());
	        System.out.println(token);

		}
	}
	
	public static void createIndexA() throws Exception{
		Directory dir = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		IndexWriter indexWriter = new IndexWriter(dir,analyzer,MaxFieldLength.LIMITED);
		for(int i=0;i<10;i++){
			Document doc = new Document();
			doc.add(new Field("name", "zhansan",Store.YES,Index.ANALYZED));
			doc.add(new Field("address","shanghai",Store.YES,Index.ANALYZED));
			doc.add(new Field("sex","man",Store.YES,Index.NOT_ANALYZED));
			doc.add(new Field("introduce","I am a man,my name is james,and I have a friend ",Store.YES,Index.ANALYZED));
			doc.add(new NumericField("size",Store.YES,true).setIntValue(i));
			indexWriter.addDocument(doc);
		}
		indexWriter.close();
	}
	
	public static void printDocumentInfo(Document doc){
		System.out.println("name: " + new String(doc.getField("name").stringValue()));
		System.out.println("address: " + new String(doc.getField("name").stringValue()));
		System.out.println("sex: " + new String(doc.getField("name").stringValue()));
		System.out.println("introduce: " + new String(doc.getField("name").stringValue()));
		System.out.println("size: " + new String(doc.getField("name").stringValue()));
	}
	
	public static void main(String[] args) throws Exception {
		createIndexA();
		searchIndex();
		testHightLight();
		testAnalyzer();
	}
	
}
