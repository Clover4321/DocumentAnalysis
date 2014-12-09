import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import wordTFIDF.WordTFIDF;
import xmlDriver.XMLDriver;

public class DocumentAnalysis {
	
	public static void wordTFIDFJob(Configuration conf, String[] args) throws Exception{
		Job job = new Job(conf);
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("word tf-idf");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(WordTFIDF.Map.class);
		//job.setCombinerClass(DocumentAnalysis.Combine.class);
		job.setReducerClass(WordTFIDF.Reduce.class);
		
		job.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		wordTFIDFJob(conf, args);
	}
}