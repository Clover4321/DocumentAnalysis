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

public class DocumentAnalysis {
	
	public static class XmlInputFormat1 extends TextInputFormat {

	    public static final String START_TAG_KEY = "xmlinput.start";
	    public static final String END_TAG_KEY = "xmlinput.end";


	    public RecordReader<LongWritable, Text> createRecordReader(
	            InputSplit split, TaskAttemptContext context) {
	        return new XmlRecordReader();
	    }

	    /**
	     * XMLRecordReader class to read through a given xml document to output
	     * xml blocks as records as specified by the start tag and end tag
	     *
	     */

	    public static class XmlRecordReader extends
	    RecordReader<LongWritable, Text> {
	        private byte[] startTag;
	        private byte[] endTag;
	        private long start;
	        private long end;
	        private FSDataInputStream fsin;
	        private DataOutputBuffer buffer = new DataOutputBuffer();

	        private LongWritable key = new LongWritable();
	        private Text value = new Text();
	        @Override
	        public void initialize(InputSplit split, TaskAttemptContext context)
	        throws IOException, InterruptedException {
	            Configuration conf = context.getConfiguration();
	            startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
	            endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
	            FileSplit fileSplit = (FileSplit) split;

	            // open the file and seek to the start of the split
	            start = fileSplit.getStart();
	            end = start + fileSplit.getLength();
	            Path file = fileSplit.getPath();
	            FileSystem fs = file.getFileSystem(conf);
	            fsin = fs.open(fileSplit.getPath());
	            fsin.seek(start);

	        }
	        @Override
	        public boolean nextKeyValue() throws IOException,
	        InterruptedException {
	            if (fsin.getPos() < end) {

	                if (readUntilMatch(startTag, false)) {
	                    try {

	                        buffer.write(startTag);
	                        if (readUntilMatch(endTag, true)) {

	                            key.set(fsin.getPos());
	                            value.set(buffer.getData(), 0,
	                                    buffer.getLength());
	                            return true;
	                        }
	                    } finally {
	                        buffer.reset();
	                    }
	                }
	            }
	            return false;
	        }
	        @Override
	        public LongWritable getCurrentKey() throws IOException,
	        InterruptedException {
	            return key;
	        }

	        @Override
	        public Text getCurrentValue() throws IOException,
	        InterruptedException {
	            return value;
	        }
	        @Override
	        public void close() throws IOException {
	            fsin.close();
	        }
	        @Override
	        public float getProgress() throws IOException {
	            return (fsin.getPos() - start) / (float) (end - start);
	        }

	        private boolean readUntilMatch(byte[] match, boolean withinBlock)
	        throws IOException {

	            int i = 0;
	            while (true) {

	                int b = fsin.read();

	                // end of file:
	                    if (b == -1)
	                        {return false;}
	                // save to buffer:
	                    if (withinBlock)
	                        buffer.write(b);
	                // check if we're matching:
	                    if (b == match[i]) {
	                        i++;
	                        if (i >= match.length)
	                            {return true;}
	                    } else
	                        i = 0;
	                    // see if we've passed the stop point:
	                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
	                        {return false;}
	            }
	        }
	    }
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		Path path_tmp = new Path("hdfs://localhost:9000/output/job1_temp");
		//job1
		try
		{
			Job job1 = new Job(conf);
			job1.setJarByClass(DocumentAnalysis.class);
			job1.setJobName("InvertedIndex_Phase1");
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setMapperClass(MapReducePhase1.Map1.class);
			job1.setReducerClass(MapReducePhase1.Reduce1.class);
			
			job1.setInputFormatClass(XmlInputFormat1.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, path_tmp); 
			job1.waitForCompletion(true);
			
			//job2
			Job job2 = new Job(conf);
			job2.setJarByClass(DocumentAnalysis.class);
			job2.setJobName("InvertedIndex_Phase2");
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setMapperClass(MapReducePhase2.Map2.class);
			job2.setReducerClass(MapReducePhase2.Reduce2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			Path job2_input = new Path("hdfs://localhost:9000/output/job1_temp/part-r-00000");
			FileInputFormat.addInputPath(job2, job2_input);
	        job2.setOutputKeyClass(Text.class);  
	        job2.setOutputValueClass(Text.class);  
	        job2.setOutputFormatClass(TextOutputFormat.class); 
			
	        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	        job2.waitForCompletion(true);
		}
		finally
		{
			
		}
		
	}
}
