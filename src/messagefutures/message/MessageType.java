package messagefutures.message;

public enum MessageType {
	BEGIN, READ, WRITE, COMMIT, ABORT, SCAN, COMMITACK, ABORTACK, LISTACTIVE, LISTPENDING, LOGPROPAGATE, ACK; 
}
