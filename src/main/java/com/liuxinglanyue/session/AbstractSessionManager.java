package com.liuxinglanyue.session;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.liuxinglanyue.session.serializer.Serializer;

public abstract class AbstractSessionManager extends ManagerBase implements Lifecycle {
	private final Log log = LogFactory.getLog(AbstractSessionManager.class);

	enum SessionPersistPolicy {
		DEFAULT, SAVE_ON_CHANGE, ALWAYS_SAVE_AFTER_REQUEST;

		static SessionPersistPolicy fromName(String name) {
			for (SessionPersistPolicy policy : SessionPersistPolicy.values()) {
				if (policy.name().equalsIgnoreCase(name)) {
					return policy;
				}
			}
			throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of "
					+ Arrays.asList(SessionPersistPolicy.values()) + ".");
		}
	}

	protected byte[] NULL_SESSION = "null".getBytes();

	// 是否开启调试
	// protected boolean debugEnabled = false;

	protected SessionHandlerValve handlerValve;
	protected ThreadLocal<CustomSession> currentSession = new ThreadLocal<>();
	protected ThreadLocal<SessionMetadata> currentSessionMetadata = new ThreadLocal<>();
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<>();
	protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<>();
	protected Serializer serializer;

	protected String serializationStrategyClass = "com.liuxinglanyue.session.serializer.JavaSerializer";

	protected EnumSet<SessionPersistPolicy> sessionPersistPoliciesSet = EnumSet.of(SessionPersistPolicy.DEFAULT);

	protected LifecycleSupport lifecycle = new LifecycleSupport(this);

	public abstract Session createEmptySession();

	protected abstract void initializeDatabaseConnection() throws LifecycleException;

	protected abstract void connectionDestroy();

	public String getSessionPersistPolicies() {
		StringBuilder policies = new StringBuilder();
		for (Iterator<SessionPersistPolicy> iter = this.sessionPersistPoliciesSet.iterator(); iter.hasNext();) {
			SessionPersistPolicy policy = iter.next();
			policies.append(policy.name());
			if (iter.hasNext()) {
				policies.append(",");
			}
		}
		return policies.toString();
	}

	public void setSessionPersistPolicies(String sessionPersistPolicies) {
		String[] policyArray = sessionPersistPolicies.split(",");
		EnumSet<SessionPersistPolicy> policySet = EnumSet.of(SessionPersistPolicy.DEFAULT);
		for (String policyName : policyArray) {
			SessionPersistPolicy policy = SessionPersistPolicy.fromName(policyName);
			policySet.add(policy);
		}
		this.sessionPersistPoliciesSet = policySet;
	}

	public boolean getSaveOnChange() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.SAVE_ON_CHANGE);
	}

	public boolean getAlwaysSaveAfterRequest() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.ALWAYS_SAVE_AFTER_REQUEST);
	}

	public void setSerializationStrategyClass(String strategy) {
		this.serializationStrategyClass = strategy;
	}

	/*
	 * public void setDebug(String debug) { this.debugEnabled =
	 * Boolean.parseBoolean(debug); }
	 */

	@Override
	public int getRejectedSessions() {
		return 0;
	}

	public void setRejectedSessions(int i) {
	}

	@Override
	public void load() throws ClassNotFoundException, IOException {

	}

	@Override
	public void unload() throws IOException {

	}

	@Override
	public void addLifecycleListener(LifecycleListener listener) {
		lifecycle.addLifecycleListener(listener);
	}

	@Override
	public LifecycleListener[] findLifecycleListeners() {
		return lifecycle.findLifecycleListeners();
	}

	@Override
	public void removeLifecycleListener(LifecycleListener listener) {
		lifecycle.removeLifecycleListener(listener);
	}

	@Override
	protected synchronized void startInternal() throws LifecycleException {
		super.startInternal();

		setState(LifecycleState.STARTING);

		boolean attachedToValve = false;
		for (Valve valve : getContainer().getPipeline().getValves()) {
			if (valve instanceof SessionHandlerValve) {
				this.handlerValve = (SessionHandlerValve) valve;
				this.handlerValve.setSessionManager(this);

				log.info("Attached to SessionHandlerValve");
				attachedToValve = true;
				break;
			}
		}

		if (!attachedToValve) {
			String error = "Unable to attach to session handling valve; sessions cannot be saved after the request without the valve starting properly.";
			log.fatal(error);
			throw new LifecycleException(error);
		}

		try {
			initializeSerializer();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			log.fatal("Unable to load serializer", e);
			throw new LifecycleException(e);
		}

		log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");

		initializeDatabaseConnection();

		setDistributable(true);
	}

	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		if (log.isDebugEnabled()) {
			log.debug("Stopping");
		}

		setState(LifecycleState.STOPPING);

		connectionDestroy();

		super.stopInternal();
	}

	protected abstract String generateCustomSessionId(String requestedSessionId);

	@Override
	public Session createSession(String requestedSessionId) {
		generateCustomSessionId(requestedSessionId);
		return currentSession.get();
	}

	protected boolean initSession(Object db, String sessionId) {
		CustomSession session = null;
		if (null != sessionId) {
			session = (CustomSession) createEmptySession();
			session.setNew(true);
			session.setValid(true);
			session.setCreationTime(System.currentTimeMillis());
			session.setMaxInactiveInterval(getMaxInactiveInterval());
			session.setId(sessionId);
			session.tellNew();
		}

		currentSession.set(session);
		currentSessionId.set(sessionId);
		currentSessionIsPersisted.set(false);
		currentSessionMetadata.set(new SessionMetadata());

		if (null != session) {
			try {
				return saveInternal(db, session, true);
			} catch (IOException ex) {
				log.error("Error saving newly created session: " + ex.getMessage());
				currentSession.set(null);
				currentSessionId.set(null);
				session = null;
			}
		}

		return false;
	}

	@Override
	public void add(Session session) {
		try {
			save(session);
		} catch (IOException ex) {
			log.warn("Unable to add to session manager store: " + ex.getMessage());
			throw new RuntimeException("Unable to add to session manager store.", ex);
		}
	}

	@Override
	public Session findSession(String id) throws IOException {
		CustomSession session = null;

		if (null == id) {
			currentSessionIsPersisted.set(false);
			currentSession.set(null);
			currentSessionMetadata.set(null);
			currentSessionId.set(null);
		} else if (id.equals(currentSessionId.get())) {
			session = currentSession.get();
		} else {
			byte[] data = loadSessionDataFromDB(id);
			if (data != null) {
				DeserializedSessionContainer container = sessionFromSerializedData(id, data);
				session = container.session;
				currentSession.set(session);
				currentSessionMetadata.set(container.metadata);
				currentSessionIsPersisted.set(true);
				currentSessionId.set(id);
			} else {
				currentSessionIsPersisted.set(false);
				currentSession.set(null);
				currentSessionMetadata.set(null);
				currentSessionId.set(null);
			}
		}

		return session;
	}

	public abstract String dbSet(Object db, byte[] key, byte[] value);

	public abstract long dbExpire(Object db, byte[] key, int seconds);

	public abstract void clear();

	public abstract int getSize() throws IOException;

	public abstract String[] keys() throws IOException;

	public abstract byte[] loadSessionDataFromDB(String id) throws IOException;

	public DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException {
		log.trace("Deserializing session " + id + " from DB");

		if (Arrays.equals(NULL_SESSION, data)) {
			log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
			throw new IOException("Serialized session data was equal to NULL_SESSION");
		}

		CustomSession session = null;
		SessionMetadata metadata = new SessionMetadata();

		try {
			session = (CustomSession) createEmptySession();

			serializer.deserializeInto(data, session, metadata);

			session.setId(id);
			session.setNew(false);
			session.setMaxInactiveInterval(getMaxInactiveInterval() * 1000);
			session.access();
			session.setValid(true);
			session.resetDirtyTracking();

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + id + "]:");
				Enumeration<String> en = session.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}
		} catch (ClassNotFoundException ex) {
			log.fatal("Unable to deserialize into session", ex);
			throw new IOException("Unable to deserialize into session", ex);
		}

		return new DeserializedSessionContainer(session, metadata);
	}

	public void save(Session session) throws IOException {
		save(session, false);
	}

	public abstract void save(Session session, boolean forceSave) throws IOException;

	protected boolean saveInternal(Object db, Session session, boolean forceSave) throws IOException {
		boolean error = true;

		try {
			log.trace("Saving session " + session + " into " + db);

			CustomSession customSession = (CustomSession) session;

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + customSession.getId() + "]:");
				Enumeration<String> en = customSession.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}

			byte[] binaryId = customSession.getId().getBytes();

			Boolean isCurrentSessionPersisted;
			SessionMetadata sessionMetadata = currentSessionMetadata.get();
			byte[] originalSessionAttributes = sessionMetadata.getSessionAttributes();
			byte[] sessionAttributes = null;
			if (forceSave || customSession.isDirty() || null == (isCurrentSessionPersisted = this.currentSessionIsPersisted.get())
					|| !isCurrentSessionPersisted
					|| !Arrays.equals(originalSessionAttributes, (sessionAttributes = serializer.attributesFrom(customSession)))) {

				log.trace("Save was determined to be necessary");

				if (null == sessionAttributes) {
					sessionAttributes = serializer.attributesFrom(customSession);
				}

				SessionMetadata updatedMetadata = new SessionMetadata();
				updatedMetadata.setSessionAttributes(sessionAttributes);

				dbSet(db, binaryId, serializer.serializeFrom(customSession, updatedMetadata));

				customSession.resetDirtyTracking();
				currentSessionMetadata.set(updatedMetadata);
				currentSessionIsPersisted.set(true);
			} else {
				log.trace("Save was determined to be unnecessary");
			}

			log.trace("Setting expire timeout on session [" + customSession.getId() + "] to " + getMaxInactiveInterval());
			dbExpire(db, binaryId, getMaxInactiveInterval());

			error = false;

			return error;
		} catch (IOException e) {
			log.error(e.getMessage());

			throw e;
		}
	}

	@Override
	public void remove(Session session) {
		remove(session, false);
	}

	@Override
	public abstract void remove(Session session, boolean update);

	public void afterRequest() {
		CustomSession customSession = currentSession.get();
		if (customSession != null) {
			try {
				if (customSession.isValid()) {
					log.trace("Request with session completed, saving session " + customSession.getId());
					save(customSession, getAlwaysSaveAfterRequest());
				} else {
					log.trace("HTTP Session has been invalidated, removing :" + customSession.getId());
					remove(customSession);
				}
			} catch (Exception e) {
				log.error("Error storing/removing session", e);
			} finally {
				currentSession.remove();
				currentSessionId.remove();
				currentSessionIsPersisted.remove();
				log.trace("Session removed from ThreadLocal :" + customSession.getIdInternal());
			}
		}
	}

	@Override
	public void processExpires() {
	}

	private void initializeSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		log.info("Attempting to use serializer :" + serializationStrategyClass);
		serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

		Loader loader = null;

		if (getContainer() != null) {
			loader = getContainer().getLoader();
		}

		ClassLoader classLoader = null;

		if (loader != null) {
			classLoader = loader.getClassLoader();
		}
		serializer.setClassLoader(classLoader);
	}

}

class DeserializedSessionContainer {
	public final CustomSession session;
	public final SessionMetadata metadata;

	public DeserializedSessionContainer(CustomSession session, SessionMetadata metadata) {
		this.session = session;
		this.metadata = metadata;
	}
}