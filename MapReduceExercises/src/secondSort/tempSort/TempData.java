package secondSort.tempSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TempData implements WritableComparable<TempData> {
	private static String delimiter = "\\s+";
	private String strDate;
	private int year;
	private int temp;

	public TempData() {
		super();
		// TODO Auto-generated constructor stub
	}

	public TempData(Text value) {
		String[] str = value.toString().split(delimiter);
		this.strDate = str[0] + " " + str[1];
		this.year = Integer.parseInt(str[0].substring(0, 4));
		this.temp = Integer.parseInt(str[2].substring(0, str[2].length() - 1));
	}

	public TempData(String strDate, int year, int temp) {
		super();
		this.strDate = strDate;
		this.year = year;
		this.temp = temp;
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
	public int compareTo(TempData o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return "TempData [strDate=" + strDate + ", year=" + year + ", temp=" + temp + "]";
	}

}
