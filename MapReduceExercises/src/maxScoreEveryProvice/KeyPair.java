package maxScoreEveryProvice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements  WritableComparable<KeyPair> {
	private String provice;
	private Long grade;
		
	public KeyPair(String provice, Long grade) {
		super();
		this.provice = provice;
		this.grade = grade;
	}
	public String getProvice() {
		return provice;
	}
	public void setProvice(String provice) {
		this.provice = provice;
	}
	public Long getGrade() {
		return grade;
	}
	public void setGrade(Long grade) {
		this.grade = grade;
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return provice.hashCode()+grade.hashCode();
	}
	@Override
	public String toString() {		 
		return provice+" "+grade.toString();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		//provice=in.readLine(); 
		provice=Text.readString(in);
		grade=in.readLong();		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		//out.writeBytes(provice);
		Text.writeString(out, provice);
		out.writeLong(grade);
	}
 
	@Override
	public int compareTo(KeyPair o) {
		if(provice.equals(o.getProvice())){
			return -grade.compareTo(o.getGrade());
		}else{
		   return provice.compareTo(o.getProvice());
	    }
	}
 
	
}
