package Writables;

import java.io.*;
import org.apache.hadoop.io.*;

public class Map1ValueWritable implements Writable{
	
	private int docid;
	private int tf;
	private int word_sum;
	
	public Map1ValueWritable() {}
	
	public Map1ValueWritable(int docid,int tf,int word_sum) {
	   set(docid, tf,word_sum);
	}

	public void set(int docid, int tf,int word_sum) {
	   this.docid = docid;
	   this.tf = tf;
	   this.word_sum = word_sum;
	}
	public int getdocid() {
	   return docid;
	}
	public int gettf() {
	   return tf;
	}
	public int getwordsum(){
		return word_sum;
	}
	@Override
	public void write(DataOutput out) throws IOException {
	   out.writeInt(docid);
	   out.writeInt(tf);
	   out.writeInt(word_sum);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
	   docid = in.readInt();
	   tf = in.readInt();
	   word_sum = in.readInt();
	}
	//public int hashCode() {}
	@Override
	public String toString() {
	   return docid+":"+tf+"/"+word_sum;
	}
}
