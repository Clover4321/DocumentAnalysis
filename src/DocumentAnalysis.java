import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.*;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

			
			
			job1.setMapperClass(DocumentAnalysis.Map1.class);
			job1.setReducerClass(DocumentAnalysis.Reduce1.class);
			
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
			job2.setMapperClass(DocumentAnalysis.Map2.class);
			job2.setReducerClass(DocumentAnalysis.Reduce2.class);
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
