package messagefutures.event;

import java.util.ArrayList;
import java.util.HashMap;

public class CommitEvent extends Event{
	private Integer TransactionNumber;
	public CommitEvent(Integer txnNumber,EventType type ,HashMap<String,HashMap<String,String>> writeSet)
	{
		super(type,writeSet);
		this.TransactionNumber = txnNumber;
	}
	public EventType getType() {
		return type;
	}
	public void setType(EventType type) {
		this.type = type;
	}
	public Integer getTransactionNumber() {
		return TransactionNumber;
	}
	public void setTransactionNumber(Integer transactionNumber) {
		TransactionNumber = transactionNumber;
	}
	
}