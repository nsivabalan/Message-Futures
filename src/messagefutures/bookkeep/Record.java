package messagefutures.bookkeep;

import java.util.HashMap;


public class Record {
	
	private HashMap<String,HashMap<String,String>> readSet;
	private HashMap<String,HashMap<String,String>> writeSet;
	
	public Record()
	{
		readSet = new HashMap<String,HashMap<String,String>>();
		writeSet = new HashMap<String,HashMap<String,String>>();
	}

	public HashMap<String, HashMap<String, String>> getReadSet() {
		return readSet;
	}

	public void setReadSet(
			  HashMap<String, HashMap<String, String>> readSet) {
		this.readSet = readSet;
	}

	public  HashMap<String, HashMap<String, String>> getWriteSet() {
		return writeSet;
	}

	public void setWriteSet(	
		HashMap<String, HashMap<String, String>> writeSet) {
		for(String row: writeSet.keySet())
		/*{
			System.out.println("row "+row);
			for(String col : writeSet.get(row).keySet())
				System.out.println(" "+col+"  "+writeSet.get(row).get(col));
		}*/
		this.writeSet = writeSet;
	}
	
	public void listReadSet()
	{
		System.out.println("Read Set ");
		for(String row : readSet.keySet())
		{
			System.out.println("Row "+row);
			HashMap<String,String> colval = readSet.get(row);
			for(String col : colval.keySet())
				System.out.println(""+col+" "+colval.get(col));
		}
	}
	
	
	public void listWriteSet()
	{
		System.out.println("Write Set ");
		for(String row : writeSet.keySet())
		{
			System.out.println("Row "+row);
			HashMap<String,String> colval = writeSet.get(row);
			for(String col : colval.keySet())
				System.out.println(""+col+" "+colval.get(col));
		}
	}
	
	
	
}
