package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Reduce1KeyWritable implements Writable{
	
	private String word;
	private int df;
	
	public Reduce1KeyWritable() {}
	
	public Reduce1KeyWritable(String word,int df) {
	   set(word,df);
	}

	public void set(String word,int df) {
	   this.word=word;
	   this.df=df;
	}
	public String getword() {
	   return word;
	}
	public int getdf() {
	   return df;
	}
	@Override
	public void write(DataOutput out) throws IOException {
	   out.writeChars(word);
	   out.writeInt(df);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
	   word = in.readUTF();
	   df = in.readInt();
	}
	//public int hashCode() {}
	@Override
	public String toString() {
	   return word+":"+df;
	}
}
