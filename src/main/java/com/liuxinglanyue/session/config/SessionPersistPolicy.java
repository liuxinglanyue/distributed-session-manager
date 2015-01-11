package com.liuxinglanyue.session.config;

import java.util.Arrays;

/**
 * session 策略
 * @author jiaojianfeng
 *
 */
public enum SessionPersistPolicy {

	DEFAULT, SAVE_ON_CHANGE, ALWAYS_SAVE_AFTER_REQUEST;

	public static SessionPersistPolicy fromName(String name) {
		for (SessionPersistPolicy policy : SessionPersistPolicy.values()) {
			if (policy.name().equalsIgnoreCase(name)) {
				return policy;
			}
		}
		throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of "
				+ Arrays.asList(SessionPersistPolicy.values()) + ".");
	}
}
