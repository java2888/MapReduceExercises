package maxTempEveryYear.MaxTempEveryYear_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair> {
	private int year;
	private int temp;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		System.out.println("Execute write(DataOutput out)" );
		out.writeInt(this.year);
		out.writeInt(this.temp); 
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Execute readFields(DataInput in)" );
		this.year = in.readInt();
		this.temp = in.readInt();
	}

	@Override
	public int compareTo(KeyPair arg0) {
		// TODO Auto-generated method stub
		int result = Integer.compare(this.year, arg0.getYear());
		if (result == 0) {
			result = -Integer.compare(this.temp, arg0.getTemp());
		}
		return result;
	}

	@Override
	public String toString() {
		return this.year + " " + this.temp;
	}

	@Override
	public int hashCode() {
		return new Integer(year + temp).hashCode();
	}

}
