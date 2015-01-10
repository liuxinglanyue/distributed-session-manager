package com.liuxinglanyue.session.serializer;

import java.io.IOException;

import com.liuxinglanyue.session.CustomSession;
import com.liuxinglanyue.session.SessionMetadata;

public interface Serializer {
	
	void setClassLoader(ClassLoader loader);

	byte[] attributesFrom(CustomSession session) throws IOException;

	byte[] serializeFrom(CustomSession session, SessionMetadata metadata) throws IOException;

	void deserializeInto(byte[] data, CustomSession session, SessionMetadata metadata) 
			throws IOException, ClassNotFoundException;
}
