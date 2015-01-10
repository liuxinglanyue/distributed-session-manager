package com.liuxinglanyue.session.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Pool;

import com.liuxinglanyue.session.AbstractSessionManager;
import com.liuxinglanyue.session.CustomSession;

public class RedisSessionManager extends AbstractSessionManager {
	private Log log = LogFactory.getLog(RedisSessionManager.class);

	protected String host = "localhost";
	protected int port = 6379;
	protected int database = 0;
	protected String password = null;
	protected int timeout = Protocol.DEFAULT_TIMEOUT;
	protected String sentinelMaster = null;
	protected Set<String> sentinelSet = null;

	/**
	 * 不可用
	 */
	@Deprecated
	protected String serverList;

	protected Pool<Jedis> connectionPool;
	
	@Deprecated
	protected Pool<ShardedJedis> shardedPool;
	protected JedisPoolConfig connectionPoolConfig = new JedisPoolConfig();

	public final static String name = "RedisSessionManager";

	protected String generateCustomSessionId(String requestedSessionId) {
		Jedis jedis = null;
		String sessionId = null;
		String jvmRoute = getJvmRoute();

		boolean error = false;
		try {
			jedis = acquireConnection();

			if (null != requestedSessionId) {
				sessionId = requestedSessionId;
				if (jvmRoute != null) {
					sessionId += '.' + jvmRoute;
				}
				if (jedis.setnx(sessionId.getBytes(), NULL_SESSION) == 0L) {
					sessionId = null;
				}
			} else {
				do {
					sessionId = generateSessionId();
					if (jvmRoute != null) {
						sessionId += '.' + jvmRoute;
					}
				} while (jedis.setnx(sessionId.getBytes(), NULL_SESSION) == 0L);
			}

			error = initSession(jedis, sessionId);
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
		return sessionId;
	}

	@Override
	public Session createEmptySession() {
		return new CustomSession(this);
	}

	protected void initializeDatabaseConnection() throws LifecycleException {
		try {
			if (getSentinelMaster() != null) {
				Set<String> sentinelSet = getSentinelSet();
				if (sentinelSet != null && sentinelSet.size() > 0) {
					connectionPool = new JedisSentinelPool(getSentinelMaster(), sentinelSet, this.connectionPoolConfig, getTimeout(), getPassword());
				} else {
					throw new LifecycleException(
							"Error configuring Redis Sentinel connection pool: expected both `sentinelMaster` and `sentiels` to be configured");
				}
			} else if (null != serverList && !"".equals(serverList)) {
				String[] servers = serverList.split(",");
				List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>(servers.length);
				for (int i = 0; i < servers.length; i++) {
					String[] hostAndPort = servers[i].split(":");
					JedisShardInfo shardInfo = new JedisShardInfo(hostAndPort[0], Integer.parseInt(hostAndPort[1]), getTimeout());
					if (hostAndPort.length == 3) {
						shardInfo.setPassword(hostAndPort[2]);
					}
					shards.add(shardInfo);
				}
				if (shards.size() == 1) {
					connectionPool = new JedisPool(this.connectionPoolConfig, shards.get(0).getHost(), shards.get(0).getPort(), shards.get(0)
							.getTimeout(), shards.get(0).getPassword());
				} else {
					shardedPool = new ShardedJedisPool(this.connectionPoolConfig, shards);
				}
			} else {
				connectionPool = new JedisPool(this.connectionPoolConfig, getHost(), getPort(), getTimeout(), getPassword());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new LifecycleException("Error connecting to Redis", e);
		}
	}

	public void save(Session session, boolean forceSave) throws IOException {
		Jedis jedis = null;
		Boolean error = true;

		try {
			jedis = acquireConnection();
			error = saveInternal(jedis, session, forceSave);
		} catch (IOException e) {
			throw e;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public byte[] loadSessionDataFromDB(String id) throws IOException {
		Jedis jedis = null;
		Boolean error = true;

		try {
			log.trace("Attempting to load session " + id + " from Redis");

			jedis = acquireConnection();
			byte[] data = jedis.get(id.getBytes());
			error = false;

			if (data == null) {
				log.trace("Session " + id + " not found in Redis");
			}

			return data;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public String dbSet(Object db, byte[] key, byte[] value) {
		if (null != db && db instanceof Jedis) {
			return ((Jedis) db).set(key, value);
		}
		throw new RuntimeException("db 为非Jedis !!!");
	}

	public long dbExpire(Object db, byte[] key, int seconds) {
		if (null != db && db instanceof Jedis) {
			return ((Jedis) db).expire(key, seconds);
		}
		throw new RuntimeException("db 为非Jedis !!!");
	}

	public void remove(Session session, boolean update) {
		Jedis jedis = null;
		Boolean error = true;

		log.trace("Removing session ID : " + session.getId());

		try {
			jedis = acquireConnection();
			jedis.del(session.getId());
			error = false;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public void clear() {
		Jedis jedis = null;
		Boolean error = true;
		try {
			jedis = acquireConnection();
			jedis.flushDB();
			error = false;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public int getSize() throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			jedis = acquireConnection();
			int size = jedis.dbSize().intValue();
			error = false;
			return size;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public String[] keys() throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			jedis = acquireConnection();
			Set<String> keySet = jedis.keys("*");
			error = false;
			return keySet.toArray(new String[keySet.size()]);
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	protected void connectionDestroy() {
		connectionPool.destroy();
	}

	protected Jedis acquireConnection() {
		Jedis jedis = connectionPool.getResource();

		if (getDatabase() != 0) {
			jedis.select(getDatabase());
		}

		return jedis;
	}

	protected void returnConnection(Jedis jedis, Boolean error) {
		if (error) {
			connectionPool.returnBrokenResource(jedis);
		} else {
			connectionPool.returnResource(jedis);
		}
	}

	protected void returnConnection(Jedis jedis) {
		returnConnection(jedis, false);
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSentinels() {
		StringBuilder sentinels = new StringBuilder();
		for (Iterator<String> iter = this.sentinelSet.iterator(); iter.hasNext();) {
			sentinels.append(iter.next());
			if (iter.hasNext()) {
				sentinels.append(",");
			}
		}
		return sentinels.toString();
	}

	public void setSentinels(String sentinels) {
		if (null == sentinels) {
			sentinels = "";
		}

		String[] sentinelArray = sentinels.split(",");
		this.sentinelSet = new HashSet<String>(Arrays.asList(sentinelArray));
	}

	public void setServerList(String serverList) {
		this.serverList = serverList;
	}

	public Set<String> getSentinelSet() {
		return this.sentinelSet;
	}

	public String getSentinelMaster() {
		return this.sentinelMaster;
	}

	public void setSentinelMaster(String master) {
		this.sentinelMaster = master;
	}

	public int getConnectionPoolMaxTotal() {
		return this.connectionPoolConfig.getMaxTotal();
	}

	public void setConnectionPoolMaxTotal(int connectionPoolMaxTotal) {
		this.connectionPoolConfig.setMaxTotal(connectionPoolMaxTotal);
	}

	public int getConnectionPoolMaxIdle() {
		return this.connectionPoolConfig.getMaxIdle();
	}

	public void setConnectionPoolMaxIdle(int connectionPoolMaxIdle) {
		this.connectionPoolConfig.setMaxIdle(connectionPoolMaxIdle);
	}

	public int getConnectionPoolMinIdle() {
		return this.connectionPoolConfig.getMinIdle();
	}

	public void setConnectionPoolMinIdle(int connectionPoolMinIdle) {
		this.connectionPoolConfig.setMinIdle(connectionPoolMinIdle);
	}

	public boolean getLifo() {
		return this.connectionPoolConfig.getLifo();
	}

	public void setLifo(boolean lifo) {
		this.connectionPoolConfig.setLifo(lifo);
	}

	public long getMaxWaitMillis() {
		return this.connectionPoolConfig.getMaxWaitMillis();
	}

	public void setMaxWaitMillis(long maxWaitMillis) {
		this.connectionPoolConfig.setMaxWaitMillis(maxWaitMillis);
	}

	public long getMinEvictableIdleTimeMillis() {
		return this.connectionPoolConfig.getMinEvictableIdleTimeMillis();
	}

	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
		this.connectionPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
	}

	public long getSoftMinEvictableIdleTimeMillis() {
		return this.connectionPoolConfig.getSoftMinEvictableIdleTimeMillis();
	}

	public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
		this.connectionPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
	}

	public int getNumTestsPerEvictionRun() {
		return this.connectionPoolConfig.getNumTestsPerEvictionRun();
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.connectionPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
	}

	public boolean getTestOnCreate() {
		return this.connectionPoolConfig.getTestOnCreate();
	}

	public void setTestOnCreate(boolean testOnCreate) {
		this.connectionPoolConfig.setTestOnCreate(testOnCreate);
	}

	public boolean getTestOnBorrow() {
		return this.connectionPoolConfig.getTestOnBorrow();
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.connectionPoolConfig.setTestOnBorrow(testOnBorrow);
	}

	public boolean getTestOnReturn() {
		return this.connectionPoolConfig.getTestOnReturn();
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.connectionPoolConfig.setTestOnReturn(testOnReturn);
	}

	public boolean getTestWhileIdle() {
		return this.connectionPoolConfig.getTestWhileIdle();
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.connectionPoolConfig.setTestWhileIdle(testWhileIdle);
	}

	public long getTimeBetweenEvictionRunsMillis() {
		return this.connectionPoolConfig.getTimeBetweenEvictionRunsMillis();
	}

	public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
		this.connectionPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
	}

	public String getEvictionPolicyClassName() {
		return this.connectionPoolConfig.getEvictionPolicyClassName();
	}

	public void setEvictionPolicyClassName(String evictionPolicyClassName) {
		this.connectionPoolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
	}

	public boolean getBlockWhenExhausted() {
		return this.connectionPoolConfig.getBlockWhenExhausted();
	}

	public void setBlockWhenExhausted(boolean blockWhenExhausted) {
		this.connectionPoolConfig.setBlockWhenExhausted(blockWhenExhausted);
	}

	public boolean getJmxEnabled() {
		return this.connectionPoolConfig.getJmxEnabled();
	}

	public void setJmxEnabled(boolean jmxEnabled) {
		this.connectionPoolConfig.setJmxEnabled(jmxEnabled);
	}

	public String getJmxNameBase() {
		return this.connectionPoolConfig.getJmxNameBase();
	}

	public void setJmxNameBase(String jmxNameBase) {
		this.connectionPoolConfig.setJmxNameBase(jmxNameBase);
	}

	public String getJmxNamePrefix() {
		return this.connectionPoolConfig.getJmxNamePrefix();
	}

	public void setJmxNamePrefix(String jmxNamePrefix) {
		this.connectionPoolConfig.setJmxNamePrefix(jmxNamePrefix);
	}

}
