package wordTFIDF;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import documentParser.TextParser;
import documentParser.XMLParser;
import Writables.*;

public class WordTFIDF {
	public static class Map extends 
	Mapper<LongWritable, Text, Text, Map1ValueWritable>
	{
		private Text mapResultKey = new Text();
		private Map1ValueWritable mapResultValue = new Map1ValueWritable();
		
		/*
		 * Input:
		 *		Key: the position of the page (useless)
		 * 		Value: the content of the page
		 * Output:
		 * 		Key: word
		 * 		Value: docid:term number
		 */
		public void map(LongWritable key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			XMLParser xmlParser = new XMLParser();
			String pageId = xmlParser.getId(value.toString());
			String pageText = xmlParser.getText(value.toString());
			TextParser textParser = new TextParser();
			String parsedText = textParser.regExpParse(pageText);
			StringTokenizer tokenizer = new StringTokenizer(parsedText);
			//initialize a dictionary, whose key is the word and value is position list
			Hashtable<String, Integer> dicHashtable = new Hashtable<String, Integer>();
			Integer tf = 0;
			int word_sum =0 ;
			String tmpKey;
			//enumerate all the words
			while (tokenizer.hasMoreTokens())
            {
                tmpKey = tokenizer.nextToken();
                tf = dicHashtable.get(tmpKey);
                //if the word is in the dic
                if (tf != null) {
					tf++;
					dicHashtable.put(tmpKey, tf);
                }
                else {
                	//the word is not in the dic
                	tf = 1;
					dicHashtable.put(tmpKey, tf);
				}
                word_sum++;
            }
			Enumeration<String> keyListEnumeration = dicHashtable.keys();
			while (keyListEnumeration.hasMoreElements()) {
				tmpKey = keyListEnumeration.nextElement();
				mapResultKey.set(tmpKey);
				mapResultValue.set(Integer.parseInt(pageId), dicHashtable.get(tmpKey).intValue(), word_sum);
				context.write(mapResultKey,mapResultValue);
			}
		}
	}
	
	public static class Reduce extends 
    Reducer<Text, Map1ValueWritable, Text, Text>
	{
		Reduce1KeyWritable reduceResultKey = new Reduce1KeyWritable();
		Text resultKey = new Text();
		Text resultValue = new Text();
		public void reduce(Text key, Iterable<Map1ValueWritable> values,
		        Context context)
		        throws IOException, InterruptedException
			{
			    int df = 0;
			    StringBuilder result =new StringBuilder("");
			    for (Map1ValueWritable val : values)
			    {
			    	result.append(val.getdocid());
			    	result.append(":");
			    	result.append(val.gettf());
			    	result.append("/");
			    	result.append(val.getwordsum());
			    	result.append(" ");
			    	df++;
			    }
			    reduceResultKey.set(key.toString(), df);
			    resultKey.set(reduceResultKey.toString());
			    resultValue.set(result.toString());
			    context.write(resultKey, resultValue);
			}
	}
}
