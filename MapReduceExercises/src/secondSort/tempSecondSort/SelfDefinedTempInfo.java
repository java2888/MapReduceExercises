package secondSort.tempSecondSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SelfDefinedTempInfo implements WritableComparable<SelfDefinedTempInfo> {
	private String strDate;
	private int year;
	private int temp;
	private static String delimiter = "\\s+";
	private static int endIndex = 4;
	private static String datePattern = "yyyy-MM-dd HH:mm:ss";

	public SelfDefinedTempInfo() {
		super();
	}

	public SelfDefinedTempInfo(String strDate, int year, int temp) {
		super();
		this.strDate = strDate;
		this.year = year;
		this.temp = temp;
	}

	public SelfDefinedTempInfo(Text value) throws ParseException {
		String[] str = value.toString().split(delimiter);
		// DateFormat df = new SimpleDateFormat(datePattern);

		this.strDate = str[0] + " " + str[1];
		this.year = Integer.parseInt(str[0].substring(0, endIndex));
		this.temp = Integer.parseInt(str[2].substring(0, str[2].length() - 1));
		System.out.println("SelfDefinedTempInfo=" + value.toString() + "; year=" + year);
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

	/*
	 * @Override public int compareTo(SelfDefinedTempInfo o) { if (this.year ==
	 * o.getYear()) { return -(this.temp - o.getTemp()); } else { return
	 * this.year - o.getYear(); } }
	 */
	@Override
	public int compareTo(SelfDefinedTempInfo o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return strDate + "," + temp + "C";
	}

}
