package MapReducePhase2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapReducePhase2 {

	public static class Map2 extends 
	Mapper<Text, Text, Text, Text>
	{
		public void map(Text key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			//System.out.println("map2");
			int splitIndex = key.toString().indexOf(",");  
            context.write(new Text(key.toString().substring(0, splitIndex)),  
                    new Text(key.toString().substring(splitIndex + 1) + ":"  
                            + value.toString()));  
		}
	}
	
	public static class Reduce2 extends 
    Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException
			{
			    Text output = new Text();
			    String result = "";
			    for (Text val : values)
			    {
			    	result += ( val.toString() + "-->" );
			    }
			    result = result.substring(0, result.length()-3);
			    output.set(result);
			    //System.out.println(output);
			    context.write(key, output);
			    //System.out.println(key+" "+output);
			}
	}
}
