package processMobileWebLog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserWebPayLoad implements Writable {
	// upPackNum¡¢downPackNum¡¢upPayLoad downPayLoad
	private int upPackNum;
	private int downPackNum;
	private int upPayLoad;
	private int downPayLoad;

	public UserWebPayLoad() {
		super();
	}

	public UserWebPayLoad(int upPackNum, int downPackNum, int upPayLoad, int downPayLoad) {
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upPackNum = in.readInt();
		downPackNum = in.readInt();
		upPayLoad = in.readInt();
		downPayLoad = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upPackNum);
		out.writeInt(downPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPayLoad);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public String toString() {
		return String.valueOf(upPackNum) + " " + String.valueOf(downPackNum) + " " + String.valueOf(upPayLoad) + " "
				+ String.valueOf(downPayLoad);

	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

}
