package messagefutures.bookkeep;

import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;

public class TransactionDetail {
	//private HbaseTranx;    
	private Integer transactionNumber;
	private Record record;
	private String clientIP;
	public String getClientIP() {
		return clientIP;
	}

	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	public Integer getLastPropTime() {
		return lastPropTime;
	}

	public void setLastPropTime(Integer lastPropTime) {
		this.lastPropTime = lastPropTime;
	}

	private Integer lastPropTime;
	//fetch LPT event number change LastPropTime to LPT
	public TransactionDetail(String clientIP,Integer txnNumber)
	{
		this.record = new Record();
		this.transactionNumber = txnNumber;
		this.clientIP = clientIP;
	}
	
	public Integer getTransactionNumber()
	{
		return transactionNumber;
	}
	
	public void setTransactionNumber(Integer txnNo)
	{
		this.transactionNumber = txnNo;
	}
	
	public void setRecord(Record record)
	{
		this.record = record;
	}
	
	public Record getRecord()
	{
		return record;
	}
	
	public void ListReadSet()
	{

		record.listReadSet();
	}
	
	public void ListWriteSet()
	{
		
		record.listWriteSet();
	}
	
	
}
