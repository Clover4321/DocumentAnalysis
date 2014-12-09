package wordTFIDF;

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

import documentParser.TextParser;
import documentParser.XMLHandler;

public class WordTFIDF {
	public static class Map extends 
	Mapper<LongWritable, Text, Text, Text>
	{
		private Text mapResultKey = new Text();
		private Text mapResultValue = new Text();
		private Hashtable<String, ArrayList<Integer>> dicHashtable;
		
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
			XMLHandler xmlHandler = new XMLHandler();
			try {
				xmlHandler.parse(value.toString());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			String pageTitle = xmlHandler.getTitle();
			String pageId = xmlHandler.getId();
			String pageText = xmlHandler.getText();
			TextParser textParser = new TextParser();
			String parsedText = textParser.regExpParse(pageText);
			StringTokenizer tokenizer = new StringTokenizer(parsedText);
			dicHashtable = new Hashtable<String, ArrayList<Integer>>();
			ArrayList<Integer> posArrayList = new ArrayList<Integer>();
			String tmpKey;
			int count=0;
			while (tokenizer.hasMoreTokens())
            {
                count++;
                //mapresultkey.set(tokenizer.nextToken() + "," + pageId);
                //position.set(String.valueOf(count));
                //context.write(mapresultkey, position);
                tmpKey = tokenizer.nextToken();
                posArrayList = dicHashtable.get(tmpKey);
                if (posArrayList != null) {
					posArrayList.add(count);
					//System.out.println(tmpKey +","+ count+" 2");
					dicHashtable.remove(tmpKey);
					dicHashtable.put(tmpKey, posArrayList);
                }
                else {
                	posArrayList = new ArrayList<Integer>();
                	//System.out.println(tmpKey +","+ count+" 1");
                	posArrayList.add(count);
					dicHashtable.put(tmpKey, posArrayList);
				}
            }
			Enumeration<String> keyListEnumeration = dicHashtable.keys();
			StringBuilder posString = new StringBuilder("");
			while (keyListEnumeration.hasMoreElements()) {
				posString= new StringBuilder("");
				tmpKey = keyListEnumeration.nextElement();
				posArrayList = dicHashtable.get(tmpKey);
				//Collections.sort(posArrayList);
				for (Integer posInteger : posArrayList)
				{
					posString.append(String.valueOf(posInteger) + ",");
				}
				posString.deleteCharAt(posString.length()-1);
				//System.out.print(tmpKey + "," + pageId);
				//System.out.println(" " + posString);
				mapResultKey.set(tmpKey);
				mapResultValue.set(pageId + ":" + posString);
				context.write(mapResultKey,mapResultValue);
			}
		}
	}
	
	public static class Combine extends 
    Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException{
			    Text position = new Text();
			    Text value = new Text();
			    String pos_str="";
			    
			    //count term frequency
			    int tf = 0;
			    ArrayList<Integer> arr = new ArrayList<Integer>();
			    for (Text val : values)
			    {
			    	tf++;
			    	//intermediate result of combining
			    	if(val.toString().contains(":"))
			    	{
			    		System.out.println("intermediate result of combining");
			    		//int pos_start_index = val.find(":");
			    		//String pos_str_split = val.toString().substring(pos_start_index+1, )
			    	}
			    	//output from Mapper
			    	arr.add(Integer.parseInt(val.toString()));
			    }
			    Collections.sort(arr);
			    int flag = 0;
			    int last_position =0;
			    for(Integer i: arr)
			    {
			    	if(flag==0) {flag=1;last_position=i;pos_str += (i.toString()+",");}
			    	else
			    	{
			    		pos_str += (String.valueOf((i.intValue()-last_position))+",");
			    		last_position = i.intValue();
			    	}
			    }
			    pos_str = pos_str.substring(0, pos_str.length()-1);
			    //pos_str += "]";
			    position.set(pos_str);
			    //change key to word
			    Text reduce_input_key = new Text();
			    String word = key.toString();
			    String[] splitter = word.split(",");
			    reduce_input_key.set(splitter[0]);
			    value.set(splitter[1]+":"+pos_str);
			    //System.out.println(reduce_input_key+"  "+splitter[1]+":"+position);
			    context.write(reduce_input_key, value);
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
			    //result = result.substring(0, result.length()-3);
			    output.set(result.toString());
			    //System.out.println(output);
			    context.write(key, output);
			}
	}
}
