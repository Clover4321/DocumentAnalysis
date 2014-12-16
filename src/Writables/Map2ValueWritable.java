package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Map2ValueWritable implements Writable{
	
	private String word;
	private double tfidf;
	
	public Map2ValueWritable() {}
	
	public Map2ValueWritable(String word,double df) {
	   set(word,df);
	}

	public void set(String word,double df) {
	   this.word=word;
	   this.tfidf=df;
	}
	public String getword() {
	   return word;
	}
	public double gettfidf() {
	   return tfidf;
	}
	@Override
	public void write(DataOutput out) throws IOException {
	   out.writeChars(word);
	   out.writeDouble(tfidf);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
	   word = in.readUTF();
	   tfidf = in.readDouble();
	}
	//public int hashCode() {}
	@Override
	public String toString() {
		DecimalFormat format = new DecimalFormat("###.###");
	   return word+":"+format.format(tfidf);
	}
}
