package categoryTFIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Writables.Map1ValueWritable;
import Writables.Reduce1KeyWritable;
import documentParser.TextParser;
import documentParser.XMLParser;

public class CategoryTFIDF {
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
		 * 		Key: category
		 * 		Value: docid:1
		 */
		public void map(LongWritable key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			XMLParser xmlParser = new XMLParser();
			String pageId = xmlParser.getId(value.toString());
			String pageText = xmlParser.getText(value.toString());
			if (pageText != null) {
				TextParser textParser = new TextParser();
				ArrayList<String> cateArrayList = textParser.getCategory(pageText);
				int catSumInOneDoc = cateArrayList.size();
				for(String category : cateArrayList)
				{
					mapResultKey.set(category);
					mapResultValue.set(Integer.parseInt(pageId), 1, catSumInOneDoc);
					context.write(mapResultKey,mapResultValue);
				}
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
