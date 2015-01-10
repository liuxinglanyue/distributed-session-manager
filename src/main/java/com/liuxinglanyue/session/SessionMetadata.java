package com.liuxinglanyue.session;

import java.io.IOException;
import java.io.Serializable;

public class SessionMetadata implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7893841019466415990L;
	
	private byte[] sessionAttributes;

	public SessionMetadata() {
		this.sessionAttributes = new byte[0];
	}

	public byte[] getSessionAttributes() {
		return sessionAttributes;
	}

	public void setSessionAttributes(byte[] sessionAttributes) {
		this.sessionAttributes = sessionAttributes;
	}

	public void copyFieldsFrom(SessionMetadata metadata) {
		this.setSessionAttributes(metadata.getSessionAttributes());
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(sessionAttributes.length);
		out.write(this.sessionAttributes);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		
		int hashLength = in.readInt();
		byte[] sessionAttributes = new byte[hashLength];
		in.read(sessionAttributes, 0, hashLength);
		
		this.sessionAttributes = sessionAttributes;
	}

}
