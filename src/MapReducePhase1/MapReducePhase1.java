package MapReducePhase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import documentParser.TextParser;
import documentParser.XMLHandler;

public class MapReducePhase1 {
	public static class Map1 extends 
	Mapper<LongWritable, Text, Text, Text>
	{
		private Text mapresultkey = new Text();
		private Text position = new Text();
		public void map(LongWritable key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			XMLHandler xmlHandler = new XMLHandler();
			//System.out.println(value.toString());
			//word.set("a");
			//context.write(word, one);
			try {
				xmlHandler.parse(value.toString());
				//System.out.println(value.toString());
				//System.out.println(xmlHandler.getId());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			String pageTitle = xmlHandler.getTitle();
			String pageId = xmlHandler.getId();
			String pageText = xmlHandler.getText();
			//System.out.println(pageText);
			TextParser textParser = new TextParser();
			String parsedText = textParser.regExpParse(pageText);
			//System.out.println(parsedText);
			StringTokenizer tokenizer = new StringTokenizer(parsedText);
            int count_position=0;
			while (tokenizer.hasMoreTokens())
            {
				count_position++;
                mapresultkey.set(tokenizer.nextToken() + "," + pageId);
                position.set(String.valueOf(count_position));
                //System.out.println(mapresultkey+" "+position);
                context.write(mapresultkey, position);
            }
		}
	}
	
	public static class Reduce1 extends 
    Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException{
			    Text position = new Text();
			    Text value = new Text();
			    String pos_str="[";
			    
			    //count term frequency
			    int tf = 0;
			    ArrayList<Integer> arr = new ArrayList<Integer>();
			    for (Text val : values)
			    {
			    	tf++;
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
			    pos_str += "]";
			    position.set(pos_str);
			    /*change key to word
			    Text reduce_input_key = new Text();
			    String word = key.toString();
			    String[] splitter = word.split(",");
			    reduce_input_key.set(splitter[0]);
			    value.set(splitter[1]+":"+pos_str);
			    System.out.println(reduce_input_key+"  "+splitter[1]+":"+position);
			    */
			    context.write(key, position);
			    //System.out.println(key+" "+position);
			}
	} 
}
