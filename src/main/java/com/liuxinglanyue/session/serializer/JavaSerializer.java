package com.liuxinglanyue.session.serializer;

import org.apache.catalina.util.CustomObjectInputStream;

import java.util.Enumeration;
import java.util.HashMap;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.liuxinglanyue.session.CustomSession;
import com.liuxinglanyue.session.SessionMetadata;

public class JavaSerializer implements Serializer {
	private ClassLoader loader;

	private final Log log = LogFactory.getLog(JavaSerializer.class);

	@Override
	public void setClassLoader(ClassLoader loader) {
		this.loader = loader;
	}

	public byte[] attributesFrom(CustomSession session) throws IOException {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		for (Enumeration<String> enumerator = session.getAttributeNames(); enumerator.hasMoreElements();) {
			String key = enumerator.nextElement();
			attributes.put(key, session.getAttribute(key));
		}

		byte[] serialized = null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));) {
			oos.writeUnshared(attributes);
			oos.flush();
			serialized = bos.toByteArray();
		}

		MessageDigest digester = null;
		try {
			digester = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.error("Unable to get MessageDigest instance for MD5");
		}
		return digester.digest(serialized);
	}

	@Override
	public byte[] serializeFrom(CustomSession session, SessionMetadata metadata) throws IOException {
		byte[] serialized = null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));) {
			oos.writeObject(metadata);
			session.writeObjectData(oos);
			oos.flush();
			serialized = bos.toByteArray();
		}

		return serialized;
	}

	@Override
	public void deserializeInto(byte[] data, CustomSession session, SessionMetadata metadata) throws IOException, ClassNotFoundException {
		try (BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
				ObjectInputStream ois = new CustomObjectInputStream(bis, loader);) {
			
			SessionMetadata serializedMetadata = (SessionMetadata) ois.readObject();
			metadata.copyFieldsFrom(serializedMetadata);
			session.readObjectData(ois);
		}
	}
}
