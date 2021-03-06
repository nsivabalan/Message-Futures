package messagefutures.bookkeep;

import java.util.HashMap;


public class Record {
	
	private HashMap<String,String> readSet;
	private HashMap<String,String> writeSet;
	
	public Record()
	{
		readSet = new HashMap<String,String>();
		writeSet = new HashMap<String,String>();
	}

	public HashMap<String, String> getReadSet() {
		return readSet;
	}

	public void setReadSet(
			  HashMap<String, String> readSet) {
		this.readSet = readSet;
	}

	public  HashMap<String, String> getWriteSet() {
		return writeSet;
	}

	public void setWriteSet(	
		HashMap<String, String> writeSet) {
		this.writeSet = writeSet;
	}
	
	public void listReadSet()
	{
		System.out.println("Read Set ");
		for(String row : readSet.keySet())
		{
			System.out.println("Row "+row+" value "+readSet.get(row));
			
		}
	}
	
	
	public void listWriteSet()
	{
		System.out.println("Write Set ");
		for(String row : writeSet.keySet())
		{
			System.out.println("Row "+row+" Value "+writeSet.get(row));
		}
	}
	
	
	
}
