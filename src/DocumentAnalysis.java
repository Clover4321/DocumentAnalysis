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

	        private boolean readUntilMatch(byte[] match, boolean writalbe)
	        throws IOException {
	            int i = 0;
	            while (true) 
	            {
	                int b = fsin.read();
	                // end of file:
	                    if (b == -1)
	                        return false;
	                // save to buffer:
	                    if (writalbe)
	                        buffer.write(b);
	                // check if we're matching:
	                    if (b == match[i]) {
	                        i++;
	                        if (i >= match.length)
	                            return true;
	                    } else
	                        i = 0;
	                    // see if we've passed the stop point:
	                    if (!writalbe && i == 0 && fsin.getPos() >= end)
	                        return false;
	            }
	        }
	    }
	}
	
	public static class Map extends 
	Mapper<LongWritable, Text, Text, Text>
	{
		private Text mapResultKey = new Text();
		private Text mapResultValue = new Text();
		private Hashtable<String, ArrayList<Integer>> dicHashtable;
		
		public void map(LongWritable key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			XMLHandler xmlHandler = new XMLHandler();
			try {
				xmlHandler.parse(value.toString());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			String pageTitle = xmlHandler.getTitle();
			String pageId = xmlHandler.getId();
			String pageText = xmlHandler.getText();
			TextParser textParser = new TextParser();
			String parsedText = textParser.regExpParse(pageText);
			StringTokenizer tokenizer = new StringTokenizer(parsedText);
			dicHashtable = new Hashtable<String, ArrayList<Integer>>();
			ArrayList<Integer> posArrayList = new ArrayList<Integer>();
			String tmpKey;
			int count=0;
			while (tokenizer.hasMoreTokens())
            {
                count++;
                //mapresultkey.set(tokenizer.nextToken() + "," + pageId);
                //position.set(String.valueOf(count));
                //context.write(mapresultkey, position);
                tmpKey = tokenizer.nextToken();
                posArrayList = dicHashtable.get(tmpKey);
                if (posArrayList != null) {
					posArrayList.add(count);
					//System.out.println(tmpKey +","+ count+" 2");
					dicHashtable.remove(tmpKey);
					dicHashtable.put(tmpKey, posArrayList);
                }
                else {
                	posArrayList = new ArrayList<Integer>();
                	//System.out.println(tmpKey +","+ count+" 1");
                	posArrayList.add(count);
					dicHashtable.put(tmpKey, posArrayList);
				}
            }
			Enumeration<String> keyListEnumeration = dicHashtable.keys();
			String posString = "";
			while (keyListEnumeration.hasMoreElements()) {
				posString = "";
				tmpKey = keyListEnumeration.nextElement();
				posArrayList = dicHashtable.get(tmpKey);
				Collections.sort(posArrayList);
				for (Integer posInteger : posArrayList)
				{
					posString += String.valueOf(posInteger) + ",";
				}
				posString = posString.substring(0,posString.length()-1);
				//System.out.print(tmpKey + "," + pageId);
				//System.out.println(" " + posString);
				mapResultKey.set(tmpKey);
				mapResultValue.set(pageId + ":" + posString);
				context.write(mapResultKey,mapResultValue);
			}
		}
	}
	
	public static class Combine extends 
    Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException{
			    Text position = new Text();
			    Text value = new Text();
			    String pos_str="";
			    
			    //count term frequency
			    int tf = 0;
			    ArrayList<Integer> arr = new ArrayList<Integer>();
			    for (Text val : values)
			    {
			    	tf++;
			    	//intermediate result of combining
			    	if(val.toString().contains(":"))
			    	{
			    		System.out.println("intermediate result of combining");
			    		//int pos_start_index = val.find(":");
			    		//String pos_str_split = val.toString().substring(pos_start_index+1, )
			    	}
			    	//output from Mapper
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
			    //pos_str += "]";
			    position.set(pos_str);
			    //change key to word
			    Text reduce_input_key = new Text();
			    String word = key.toString();
			    String[] splitter = word.split(",");
			    reduce_input_key.set(splitter[0]);
			    value.set(splitter[1]+":"+pos_str);
			    //System.out.println(reduce_input_key+"  "+splitter[1]+":"+position);
			    context.write(reduce_input_key, value);
			}
	} 
	
	public static class Reduce extends 
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
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(DocumentAnalysis.Map.class);
		//job.setCombinerClass(DocumentAnalysis.Combine.class);
		job.setReducerClass(DocumentAnalysis.Reduce.class);
		
		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
		
	}
}