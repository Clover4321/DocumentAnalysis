package wordTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.InitialContext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

import documentParser.TextParser;
import documentParser.XMLParser;

public class CountWordInitial {
	public static class Map extends 
	Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static int[] list = new int[128];
		
		public void map(LongWritable key, Text value,Context context)
			       throws IOException,InterruptedException
		{
			list[value.charAt(0)]++;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < 128; i++) {
				if (list[i] != 0) {
					context.write(new Text(String.valueOf((char)i)), new IntWritable(list[i]));
				}
			}
		}
	}
	
	public static class PPartition extends Partitioner<Text, IntWritable>{ 
		
	@Override
	public int getPartition(Text key, IntWritable value, int partNum) {
			return  0;
		}
	}
	
	public static class Reduce extends 
    Reducer<Text, IntWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values,
		        Context context)
		        throws IOException, InterruptedException
			{
				int sum = 0;
	            for (LongWritable i : values)
	            {
	            	sum += i.get();
	            }
	            context.write(key, new LongWritable(sum));
			}
	}
}
