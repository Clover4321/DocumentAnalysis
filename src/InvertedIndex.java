import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import MapReducePhase1.MapReducePhase1;
import MapReducePhase2.MapReducePhase2;
import xmlDriver.XMLDriver;

public class InvertedIndex {
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		//Path path_tmp = new Path("hdfs://localhost:9000/output/job1_temp");
		//job1
		try
		{
			Job job1 = new Job(conf);
			job1.setJarByClass(InvertedIndex.class);
			job1.setJobName("InvertedIndex_Phase1");
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setMapperClass(MapReducePhase1.Map1.class);
			job1.setReducerClass(MapReducePhase1.Reduce1.class);
			
			job1.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1])); 
			job1.waitForCompletion(true);
			
			//job2
			Job job2 = new Job(conf);
			job2.setJarByClass(InvertedIndex.class);
			job2.setJobName("InvertedIndex_Phase2");
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setMapperClass(MapReducePhase2.Map2.class);
			job2.setReducerClass(MapReducePhase2.Reduce2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			//Path job2_input = new Path("hdfs://localhost:9000/output/job1_temp/part-r-00000");
			FileInputFormat.addInputPath(job2, new Path(args[1]));
	        job2.setOutputKeyClass(Text.class);  
	        job2.setOutputValueClass(Text.class);  
	        job2.setOutputFormatClass(TextOutputFormat.class); 
			
	        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	        job2.waitForCompletion(true);
		}
		finally
		{
			
		}
		
	}
}
