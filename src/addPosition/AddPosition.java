package addPosition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import documentParser.TextParser;
import documentParser.XMLParser;

public class AddPosition {
	public static class Map extends 
	Mapper<LongWritable, Text, Text, Text>
	{
		private Text mapResultKey = new Text();
		private Text mapResultValue = new Text();
		
		
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
			Hashtable<String, StringBuilder> dicHashtable = new Hashtable<String, StringBuilder>();
			StringBuilder posList = new StringBuilder();
			String tmpKey;
			int count=0;
			//enumerate all the words
			while (tokenizer.hasMoreTokens())
            {
                count++;
                tmpKey = tokenizer.nextToken();
                posList = dicHashtable.get(tmpKey);
                //if the word is in the dic
                if (posList != null) {
					posList.append(count);
					posList.append(",");
					dicHashtable.put(tmpKey, posList);
                }
                else {
                	//the word is not in the dic
                	posList = new StringBuilder();
                	posList.append(count);
                	posList.append(",");
					dicHashtable.put(tmpKey, posList);
				}
            }
			Enumeration<String> keyListEnumeration = dicHashtable.keys();
			while (keyListEnumeration.hasMoreElements()) {
				tmpKey = keyListEnumeration.nextElement();
				posList = dicHashtable.get(tmpKey);
				posList.deleteCharAt(posList.length()-1);
				mapResultKey.set(tmpKey);
				mapResultValue.set(pageId + ":" + posList);
				context.write(mapResultKey,mapResultValue);
			}
		}
	}
	
	public static class Reduce extends 
    Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException
			{
			    Text output = new Text();
			    StringBuilder result = new StringBuilder("");
			    for (Text val : values)
			    {
			    	result.append(val.toString());
			    	result.append(" ");
			    }
			    output.set(result.toString());
			    context.write(key, output);
			}
	}
}
