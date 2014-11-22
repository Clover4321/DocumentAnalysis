import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import documentParser.TextParser;
import documentParser.XMLHandler;

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
	                        return false;
	                // save to buffer:
	                    if (withinBlock)
	                        buffer.write(b);
	                // check if we're matching:
	                    if (b == match[i]) {
	                        i++;
	                        if (i >= match.length)
	                            return true;
	                    } else
	                        i = 0;
	                    // see if we've passed the stop point:
	                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
	                        return false;
	            }
	        }
	    }
	}
	
	public static class Map extends 
	Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value,
				Context context)
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
			while (tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
		}
	}
	
	public static class Reduce extends 
    Reducer<Text, IntWritable, Text, IntWritable>
	{
		
		public void reduce(Text key, Iterable<IntWritable> values,
		        Context context)
		        throws IOException, InterruptedException
			{
			    int sum = 0;
			    for (IntWritable val : values)
			    {
			    	sum += val.get();
			    }
			    context.write(key, new IntWritable(sum));
			}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		
		Job job = new Job(conf);
		job.setJarByClass(DocumentAnalysis.class);
		job.setJobName("document analysis");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(DocumentAnalysis.Map.class);
		job.setReducerClass(DocumentAnalysis.Reduce.class);
		
		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
