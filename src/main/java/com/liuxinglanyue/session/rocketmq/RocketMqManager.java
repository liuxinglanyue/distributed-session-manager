package com.liuxinglanyue.session.rocketmq;

import java.util.List;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.liuxinglanyue.session.redis.share.KryoSerializer;

public class RocketMqManager extends StandardManager {
	private final Log log = LogFactory.getLog(RocketMqManager.class);

	public final String jvmRoute = getJvmRoute();
	
	public final String producerGroup = "TS_Producer:" + jvmRoute;
	public final String consumerGroup = "TS_Consumer:" + jvmRoute;
	
	public final String topic = "TS";
	
	public final String add_session = "ADD_Session:";
	public final String del_session = "DEL_Session:";
	public final String add_attribute = "ADD_Attribute";
	public final String del_attribute = "DEL_Attribute";
	public final String expire_session = "EXP_Session";
	
	public final String tag_add_session = add_session + jvmRoute;
	public final String tag_del_session = del_session + jvmRoute;
	public final String tag_add_attribute = add_attribute + jvmRoute;
	public final String tag_del_attribute = del_attribute + jvmRoute;
	public final String tag_exp_session = expire_session + jvmRoute;
	
	protected DefaultMQProducer producer;
	protected DefaultMQPushConsumer consumer;
	
	protected String namesrvAddr = "112.124.114.224:9876";
	protected boolean debug = false;
	
	@Override
	protected StandardSession getNewSession() {
		return new RocketMqSession(this);
	}
	
	@Override
	protected void startInternal() throws LifecycleException {
		super.startInternal();
		
		startProducer();
		startConsumer();
	}
	
	private void startProducer() {
		producer = new DefaultMQProducer(producerGroup);
		producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName("Producer");
        try {
			producer.start();
		} catch (MQClientException e) {
			throw new RuntimeException("RocketMQ Producer start error", e);
		}
	}

	private void startConsumer() {
		consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName("Consumer");
        try {
			consumer.subscribe(topic, "*");
			
			//register listener
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				 
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
					if(debug) {
						log.info(Thread.currentThread().getName() + " Receive New Messages: " + msgs.size());
					}
					
					for(MessageExt msg : msgs) {
						if(topic.equalsIgnoreCase(msg.getTopic())) {
							String tag = msg.getTags();
							if(tag.endsWith(jvmRoute)) {
								continue;
							}
							
							Session session = (Session) KryoSerializer.read(msg.getBody());
							if(tag.startsWith(add_session)) {
								addWithOutSend(session);
							} else if(tag.startsWith(del_session)) {
								removeWithOutSend(session, false);
							} else if(tag.startsWith(add_attribute)) {
								addWithOutSend(session);
							} else if(tag.startsWith(del_attribute)) {
								addWithOutSend(session);
							} else if(tag.startsWith(expire_session)) {
								addWithOutSend(session);
							}
							
						} else {
							log.error(Thread.currentThread().getName() + " Receive Error Messages, Topic:" + msg.getTopic());
						}
					}
					
	                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
	        });
			
			consumer.start();
		} catch (MQClientException e) {
			throw new RuntimeException("RocketMQ Consumer start error", e);
		}
	}
	
	@Override
    public void add(Session session) {
		super.add(session);
		
		send(session, tag_add_session);
	}
	
	@Override
    public void remove(Session session, boolean update) {
		super.remove(session, update);
		
		send(session, tag_del_session);
	}
	
	@Override
    public void changeSessionId(Session session) {
		super.changeSessionId(session);
		
		send(session, tag_add_session);
	}
	
	public SendResult send(Session session, String tag) {
		byte[] body = KryoSerializer.write(session);
		Message msg = new Message(topic, tag, session.getIdInternal(), body);
        try {
			return producer.send(msg);
		} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
			log.error("网络错误！", e);
		}
        
        return null;
	}
	
    public void removeWithOutSend(Session session, boolean update) {
    	super.remove(session, update);
    }
	
	public void addWithOutSend(Session session) {
		super.add(session);
	}

	@Override
    protected void stopInternal() throws LifecycleException {
		super.stopInternal();
		
		producer.shutdown();
		consumer.shutdown();
	}
	
	
	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
}
