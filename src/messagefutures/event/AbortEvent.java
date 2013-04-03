package messagefutures.event;

import java.util.ArrayList;

public class AbortEvent extends Event{
	private Integer TransactionNumber;
	
	public AbortEvent(Integer txnNumber,EventType type,String source)
	{
		super(type,source);
		this.TransactionNumber = txnNumber;
	}

	public Integer getTransactionNumber() {
		return TransactionNumber;
	}

	public void setTransactionNumber(Integer transactionNumber) {
		TransactionNumber = transactionNumber;
	}
}
