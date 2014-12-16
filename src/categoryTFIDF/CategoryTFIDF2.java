package categoryTFIDF;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CategoryTFIDF2 {
	public static class Map extends 
	Mapper<Text, Text, Text, Text>
	{
		Text mapResultKey = new Text();
		Text mapResultValue = new Text();
		DecimalFormat dFormat = new DecimalFormat("###.########");
		/*
		 * Input:
		 *		Key: category:DF
		 * 		Value: docid1:tf1/catSum1 docid2:tf2/catSum2 .....docidn:tfn/catSumN
		 * Output:
		 * 		Key: category
		 * 		Value: docid:tfidf
		 */
		public void map(Text key, Text value,Context context)
		       throws IOException,InterruptedException
		{
			Double D = Double.parseDouble(context.getConfiguration().get("D"));
			String val = value.toString();
			//split key
			String[] split = key.toString().split(":");
			if (split.length > 2) {
				return;
			}
			String word = split[0];
			Double df = Double.parseDouble(split[1]);
			//read and split value string sequentially
			int cursor = 0;
			for(int i=0;i<df;i++) //do df rounds
			{
				StringBuilder docid_tf = new StringBuilder();
				while(val.charAt(cursor)!=' ')
				{
					docid_tf.append(val.charAt(cursor));
					cursor++;
				}
				cursor++;
				int split_index = docid_tf.indexOf(":");
				String docid = docid_tf.substring(0,split_index);
				String tf_wordsum = docid_tf.substring(split_index+1, docid_tf.length());
				split_index = tf_wordsum.indexOf("/");
				double tf =Double.parseDouble(tf_wordsum.substring(0, split_index));
				double word_sum = Double.parseDouble(tf_wordsum.substring(split_index+1));
				//compute TF-IDF
				//do tf/word_sum*log(D/df)
				double tfidf = tf*Math.log(D/df)/word_sum;
				
				mapResultKey.set(docid);
				mapResultValue.set(word+":"+dFormat.format(tfidf));
				context.write(mapResultKey, mapResultValue);
				//System.out.println(mapResultKey.toString());
				//System.out.println(mapResultValue.toString());
			}
		}
	}
	
	public static class Reduce extends     
	Reducer<Text, Text, Text, Text>
	{
		Text reduceResultValue = new Text();
		int word_count =0;
		public void reduce(Text key, Iterable<Text> values,
		        Context context)
		        throws IOException, InterruptedException
			{
			    //word_count++;
			    for(Text val : values)
			    {
			    	reduceResultValue.set(val.toString());
				    context.write(key, reduceResultValue);
			    	//System.out.println(result.toString());
			    }
			    //System.out.println(result.toString());
			}
	}
}
