import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import categoryTFIDF.*;
import addPosition.AddPosition;
import wordTFIDF.*;
import Writables.*;
import xmlDriver.XMLDriver;

public class DocumentAnalysis {
	
	public static void wordTFIDFJob(Configuration conf, String[] args) throws Exception{
		
		
		Job job = new Job(conf,"InvertedIndex");
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("word tf-idf");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Map1ValueWritable.class);
		
		job.setMapperClass(WordTFIDF.Map.class);
		//job.setCombinerClass(WordTFIDF.Reduce.class);
		job.setReducerClass(WordTFIDF.Reduce.class);
		
		job.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
        
        //Phase 2
        Job job2 = new Job(conf,"TFIDF");
		job2.setJarByClass(DocumentAnalysis.class);
		job2.setJobName("word tf-idf2");
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(WordTFIDF2.Map.class);
		//job.setCombinerClass(WordTFIDF.Reduce.class);
		job2.setReducerClass(WordTFIDF2.Reduce.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        System.exit(job2.waitForCompletion(true)?0:1);
	}
	
	public static void categoryTFIDFJob(Configuration conf, String[] args) throws Exception{
		
		
		Job job = new Job(conf,"Category tfidf");
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("Category tfidf");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Map1ValueWritable.class);
		
		job.setMapperClass(CategoryTFIDF.Map.class);
		//job.setCombinerClass(WordTFIDF.Reduce.class);
		job.setReducerClass(CategoryTFIDF.Reduce.class);
		
		job.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
        
        //Phase 2
        Job job2 = new Job(conf,"category TFIDF2");
		job2.setJarByClass(DocumentAnalysis.class);
		job2.setJobName("category tf-idf2");
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(CategoryTFIDF2.Map.class);
		//job.setCombinerClass(WordTFIDF.Reduce.class);
		job2.setReducerClass(CategoryTFIDF2.Reduce.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        System.exit(job2.waitForCompletion(true)?0:1);
	}
	
	public static void wordTFIDFwithPosJob(Configuration conf, String[] args) throws Exception{
		Job job = new Job(conf);
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("word tf-idf with pos");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(AddPosition.Map.class);
		//job.setCombinerClass(WordTFIDF.Reduce.class);
		job.setReducerClass(AddPosition.Reduce.class);
		
		job.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static void CountDocsJob(Configuration conf, String[] args) throws Exception{
		Job job = new Job(conf);
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("count doc number");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(CountDocNum.Map.class);
		job.setCombinerClass(CountDocNum.Reduce.class);
		job.setPartitionerClass(CountDocNum.PPartition.class);
		job.setReducerClass(CountDocNum.Reduce.class);
		
		job.setInputFormatClass(XMLDriver.XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
	}

	public static void wordInitialJob(Configuration conf, String[] args) throws Exception{
		Job job = new Job(conf);
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("count word initial number");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(CountWordInitial.Map.class);
		job.setCombinerClass(CountWordInitial.Reduce.class);
		job.setPartitionerClass(CountWordInitial.PPartition.class);
		job.setReducerClass(CountWordInitial.Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
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
		conf.set("D", "4478");
		categoryTFIDFJob(conf, args);
	}
}