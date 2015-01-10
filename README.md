# distributed-session-manager

基于Tomcat7

redis配置：
<Valve className="com.liuxinglanyue.session.SessionHandlerValve" />
<Manager pathname="" className="com.liuxinglanyue.session.redis.RedisSessionManager" 
		 host="127.0.0.1"
         port="6379"
         database="0"
         maxInactiveInterval="60"
         sessionPersistPolicies="SAVE_ON_CHANGE,ALWAYS_SAVE_AFTER_REQUEST"
/>