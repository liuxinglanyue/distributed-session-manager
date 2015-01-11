package com.liuxinglanyue.session.redis.share;

import java.io.IOException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.exceptions.JedisConnectionException;


public class RedisManager extends StandardManager {
	private final Log log = LogFactory.getLog(RedisManager.class);

	public static final String TOMCAT_SESSION_PREFIX = "TS:";
	protected final static String NAME = RedisManager.class.getSimpleName();
    private final static String INFO = NAME + "/1.0";
    
	static ShardedJedisPool _shardedPool = null;
	static JedisPool _pool = null;
	protected JedisPoolConfig connectionPoolConfig = new JedisPoolConfig();
	// ->---------------属性----------------------
	boolean debugEnabled = false;
	private String debug = "false"; // 是否打开调试模式
	
	@Override
    public String getInfo() {
        return INFO;
    }
	
    @Override
    public String getName() {
        return NAME;
    }

	public String getDebug() {
		return debug;
	}

	public void setDebug(String debug) {
		this.debug = debug;
	}

	boolean stickySessionEnabled = true;
	private String stickySession = "true"; // 是否打开会话粘连模式

	public String getStickySession() {
		return stickySession;
	}

	public void setStickySession(String stickySession) {
		this.stickySession = stickySession;
	}

	protected String serverlist = "127.0.0.1:6379"; // 用逗号(,)分隔的"ip:port"列表

	/**
	 * the list of all cache servers;用逗号(,)分隔的"ip:port"列表
	 * 
	 * @return
	 */
	public String getServerlist() {
		return serverlist;
	}

	/**
	 * the list of all cache servers;用逗号(,)分隔的"ip:port"列表
	 * 
	 * @param serverlist
	 */
	public void setServerlist(String serverlist) {
		this.serverlist = serverlist;
	}

	protected String minConn = "5";

	public String getMinConn() {
		return minConn;
	}

	public void setMinConn(String minConn) {
		this.minConn = minConn;
	}

	protected String maxConn = "100";

	public String getMaxConn() {
		return maxConn;
	}

	public void setMaxConn(String maxConn) {
		this.maxConn = maxConn;
	}

	protected String socketTO = "6000";

	public String getSocketTO() {
		return socketTO;
	}

	public void setSocketTO(String socketTO) {
		this.socketTO = socketTO;
	}

	// <----------------属性----------------------

	public RedisManager() {
		super();
		initRedisPoolConfig();
	}

	@Override
	public Session findSession(String id) throws IOException {
		Session session = super.findSession(id);
		if (!this.isStarted()) {
			return session;
		}
		if (session == null && id != null) { // 说明session有可能在另一个节点上
			try {
				boolean idExists = jedisExists(TOMCAT_SESSION_PREFIX + id);
				if (idExists) { // Redis里有Session ID
					if (this.debugEnabled) {
						log.info("cached found and local not! id=" + id);
					}

					// ->
					RedisSession redisSession = new RedisSession(this);
					redisSession.setNew(false);
					redisSession.setValid(true);
					redisSession.setCreationTime(System.currentTimeMillis());
					redisSession.setMaxInactiveInterval(this.maxInactiveInterval);
					redisSession.setCachedId(id);

					this.add(redisSession);
					sessionCounter++;

					return redisSession;
					// <-
				}
			} catch (Exception ex) {
				log.error("error:", ex);
			}
		}

		return session;
	}

	@Override
	public Session createSession(String sessionId) {
		Session session = super.createSession(sessionId);
		if (!this.isStarted()) {
			return session;
		}

		sessionId = session.getId();
		if (this.debugEnabled) {
			log.info("id=" + sessionId);
		}

		return session;
	}

	/**
	 * 间接被createSession()调用.
	 */
	@Override
	protected StandardSession getNewSession() {
		return new RedisSession(this);
	}

	@Override
	public void remove(Session session) {
		if (this.debugEnabled) {
			log.info("id=" + session.getId());
		}
		super.remove(session);
		if (!this.isStarted()) {
			return;
		}

		try {
			jedisDel(TOMCAT_SESSION_PREFIX + session.getId());
		} catch (Exception ex) {
			log.error("error:", ex);
		}
	}

	public boolean isStarted() {
		return (super.getState() == LifecycleState.STARTED);
	}

	@Override
	protected void startInternal() throws LifecycleException {
		super.startInternal();

		debugEnabled = Boolean.parseBoolean(debug);
		stickySessionEnabled = Boolean.parseBoolean(stickySession);

		synchronized (RedisManager.class) {
			try {
				if (_shardedPool == null && _pool == null) {
					try {
						String[] servers = serverlist.split(",");
						java.util.List<JedisShardInfo> shards = new java.util.ArrayList<JedisShardInfo>(servers.length);
						for (int i = 0; i < servers.length; i++) {
							String[] hostAndPort = servers[i].split(":");
							JedisShardInfo shardInfo = new JedisShardInfo(
									hostAndPort[0],
									Integer.parseInt(hostAndPort[1]),
									Integer.valueOf(socketTO));
							if (hostAndPort.length == 3) {
								shardInfo.setPassword(hostAndPort[2]);
							}
							shards.add(shardInfo);
						}

						if (shards.size() == 1) {
							_pool = new JedisPool(connectionPoolConfig, shards.get(0)
									.getHost(), shards.get(0).getPort(), shards
									.get(0).getTimeout(), shards.get(0)
									.getPassword());
							log.info("使用:JedisPool");
						} else {
							_shardedPool = new ShardedJedisPool(connectionPoolConfig, shards);
							log.info("使用:ShardedJedisPool");
						}

						log.info("RedisShards:" + shards.toString());
						log.info("初始化RedisManager:" + this.toString());
					} catch (Exception ex) {
						log.error("error:", ex);
					}
				}

			} catch (Exception ex) {
				log.error("error:", ex);
			}
		}
	}

	@Override
	protected void stopInternal() throws LifecycleException {
		try {
			synchronized (RedisManager.class) {
				if (_shardedPool != null) {
					ShardedJedisPool myPool = _shardedPool;
					_shardedPool = null;
					try {
						myPool.destroy();
						log.info("销毁RedisManager:" + this.toString());
					} catch (Exception ex) {
						log.error("error:", ex);
					}

				}

				if (_pool != null) {
					JedisPool myPool = _pool;
					_pool = null;
					try {
						myPool.destroy();
						log.info("销毁RedisManager:" + this.toString());
					} catch (Exception ex) {
						log.error("error:", ex);
					}

				}
			}
		} finally {
			super.stopInternal();
		}
	}

	@Override
	public String toString() {
		return "RedisManager{" + "stickySession=" + stickySession + ",debug="
				+ debug + ",serverlist=" + serverlist + ",minConn=" + minConn
				+ ",maxConn=" + maxConn + ",socketTO=" + socketTO + '}';
	}

	//---------------------------------------- redis operate
	public Boolean jedisExists(String key) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.exists(key);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				return jedis.exists(key);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	Long jedisExpire(String key, int seconds) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.expire(key, seconds);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				return jedis.expire(key, seconds);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	java.util.List<Object> jedisMulti(String key,
			TransactionBlock jedisTransaction) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.multi(jedisTransaction);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				byte[] bytesKey = key.getBytes(Protocol.CHARSET);
				Jedis jedisA = jedis.getShard(bytesKey);
				return jedisA.multi(jedisTransaction);
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}

	}

	java.util.List<Object> jedisPipelined(String key,
			PipelineBlock pipelineBlock) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.pipelined(pipelineBlock);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				byte[] bytesKey = key.getBytes(Protocol.CHARSET);
				Jedis jedisA = jedis.getShard(bytesKey);
				return jedisA.pipelined(pipelineBlock);
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}

	}

	byte[] jedisHget(String hkey, String field) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.hget(hkey.getBytes(Protocol.CHARSET),
						field.getBytes(Protocol.CHARSET));
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				return jedis.hget(hkey.getBytes(Protocol.CHARSET),
						field.getBytes(Protocol.CHARSET));
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	public Long jedisHset(String hkey, String field, byte[] value) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.hset(hkey.getBytes(Protocol.CHARSET),
						field.getBytes(Protocol.CHARSET), value);
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				return jedis.hset(hkey.getBytes(Protocol.CHARSET),
						field.getBytes(Protocol.CHARSET), value);
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	public Long jedisHdel(String hkey, String field) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.hdel(hkey, field);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				return jedis.hdel(hkey, field);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	Long jedisDel(String key) {
		if (_pool != null) {
			Jedis jedis = null;
			try {
				jedis = _pool.getResource();
				return jedis.del(key.getBytes(Protocol.CHARSET));
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_pool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		} else {
			ShardedJedis jedis = null;
			try {
				jedis = _shardedPool.getResource();
				byte[] bytesKey = key.getBytes(Protocol.CHARSET);
				Jedis jedisA = jedis.getShard(bytesKey);
				return jedisA.del(bytesKey);
			} catch (IOException e) {
				throw new JedisConnectionException(e);
			} finally {
				if (jedis != null) {
					try {
						_shardedPool.returnResource(jedis);
					} catch (Throwable thex) {
					}
				}
			}
		}
	}

	//------------------------------------------ redis config
	
	private void initRedisPoolConfig() {
		connectionPoolConfig.setMinIdle(5);
		connectionPoolConfig.setMaxIdle(20);
		connectionPoolConfig.setTestOnBorrow(true);
		connectionPoolConfig.setTestOnReturn(false);
		connectionPoolConfig.setTestWhileIdle(false);
		connectionPoolConfig.setMinEvictableIdleTimeMillis(1000L * 60L * 10L); // 空闲对象,空闲多长时间会被驱逐出池里
		connectionPoolConfig.setTimeBetweenEvictionRunsMillis(1000L * 30L); // 驱逐线程30秒执行一次
		connectionPoolConfig.setNumTestsPerEvictionRun(-1); // -1,表示在驱逐线程执行时,测试所有的空闲对象
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