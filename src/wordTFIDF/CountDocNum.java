package wordTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import documentParser.TextParser;
import documentParser.XMLParser;

public class CountDocNum {
	public static class Map extends 
	Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text("num");
		
		public void map(LongWritable key, Text value,Context context)
			       throws IOException,InterruptedException
			{
				context.write(word,one);
			}
	}
	
	public static class PPartition extends Partitioner<Text, LongWritable>{ 
		
	@Override
	public int getPartition(Text key, LongWritable value, int partNum) {
			return  0;
		}
	}
	
	public static class Reduce extends 
    Reducer<Text, LongWritable, Text, LongWritable>
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
