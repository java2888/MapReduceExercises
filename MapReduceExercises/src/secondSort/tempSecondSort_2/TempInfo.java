package secondSort.tempSecondSort_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TempInfo implements WritableComparable<TempInfo> {

	private String strDate;
	private int year;
	private int temp;
	private static String delimiter = "\\s+";

	public TempInfo() {
		super();
		// TODO Auto-generated constructor stub
	}

	public TempInfo(String strDate, int year, int temp) {
		super();
		this.strDate = strDate;
		this.year = year;
		this.temp = temp;
	}

	public TempInfo(Text value) {
		String[] str = value.toString().split(delimiter);
		this.strDate = str[0] + " " + str[1];
		this.year = Integer.parseInt(str[0].substring(0, 4));
		this.temp = Integer.parseInt(str[2].substring(0, str[2].length() - 1));
		System.out.println("value="+value.toString());
		System.out.println("strDate="+strDate);
		/*
		 * super(); this.strDate = strDate; this.year = year; this.temp = temp;
		 */
	}

	public String getStrDate() {
		return strDate;
	}

	public void setStrDate(String strDate) {
		this.strDate = strDate;
	}

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
	public void readFields(DataInput in) throws IOException {
		this.strDate = in.readUTF();
		this.year = in.readInt();
		this.temp = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(strDate);
		out.writeInt(year);
		out.writeInt(temp);
	}

	@Override
	public int compareTo(TempInfo o) {

		return 0;
	}

	@Override
	public String toString() {
		return strDate + "," + year + "," + temp;
	}

}
