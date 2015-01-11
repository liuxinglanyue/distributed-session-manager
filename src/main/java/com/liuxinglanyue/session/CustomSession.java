package com.liuxinglanyue.session;

import java.io.IOException;
import java.security.Principal;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class CustomSession extends StandardSession {
	private final static Log logger = LogFactory.getLog(CustomSession.class);

	private static final long serialVersionUID = 1L;

	protected Boolean dirty;

	protected static Boolean manualDirtyTrackingSupportEnabled = false;

	public CustomSession(Manager manager) {
		super(manager);
		resetDirtyTracking();
	}

	public static void setManualDirtyTrackingSupportEnabled(Boolean enabled) {
		manualDirtyTrackingSupportEnabled = enabled;
	}

	protected static String manualDirtyTrackingAttributeKey = "__changed__";

	public static void setManualDirtyTrackingAttributeKey(String key) {
		manualDirtyTrackingAttributeKey = key;
	}

	public Boolean isDirty() {
		return dirty;
	}

	public void resetDirtyTracking() {
		dirty = false;
	}

	@Override
	public void setAttribute(String key, Object value) {
		if (manualDirtyTrackingSupportEnabled && manualDirtyTrackingAttributeKey.equals(key)) {
			dirty = true;
			return;
		}

		Object oldValue = getAttribute(key);
		super.setAttribute(key, value);

		if ((value != null || oldValue != null)
				&& (value == null && oldValue != null || oldValue == null && value != null || !value.getClass().isInstance(oldValue) || !value
						.equals(oldValue))) {
			if (this.manager instanceof AbstractSessionManager && ((AbstractSessionManager) this.manager).getSaveOnChange()) {
				try {
					((AbstractSessionManager) this.manager).save(this, true);
				} catch (IOException ex) {
					logger.error("Error saving session on setAttribute (triggered by saveOnChange=true): " + ex.getMessage());
				}
			} else {
				dirty = true;
			}
		}
	}

	@Override
	public void removeAttribute(String name) {
		super.removeAttribute(name);
		if (this.manager instanceof AbstractSessionManager && ((AbstractSessionManager) this.manager).getSaveOnChange()) {
			try {
				((AbstractSessionManager) this.manager).save(this, true);
			} catch (IOException ex) {
				logger.error("Error saving session on setAttribute (triggered by saveOnChange=true): " + ex.getMessage());
			}
		} else {
			dirty = true;
		}
	}

	@Override
	public void setId(String id) {
		this.id = id;
	}

	@Override
	public void setPrincipal(Principal principal) {
		dirty = true;
		super.setPrincipal(principal);
	}

	@Override
	public void writeObjectData(java.io.ObjectOutputStream out) throws IOException {
		super.writeObjectData(out);
		out.writeLong(this.getCreationTime());
	}

	@Override
	public void readObjectData(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readObjectData(in);
		this.setCreationTime(in.readLong());
	}

}
