package com.liuxinglanyue.session.rocketmq;

import java.security.Principal;

import org.apache.catalina.SessionListener;
import org.apache.catalina.session.StandardSession;

public class RocketMqSession extends StandardSession {
	
	private static final long serialVersionUID = 1L;
	
	protected transient RocketMqManager _manager;

	public RocketMqSession(RocketMqManager manager) {
		super(manager);
		
		this._manager = manager;
	}
	
	@Override
	public void setAttribute(String name, Object value, boolean notify) {
		super.setAttribute(name, value, notify);
		
		_manager.send(this, _manager.tag_add_attribute);
	}
	
	@Override
	protected void removeAttributeInternal(String name, boolean notify) {
		super.removeAttributeInternal(name, notify);
		
		_manager.send(this, _manager.tag_del_attribute);
	}
	
	@Override
	public void expire(boolean notify) {
		super.expire(notify);
		
		_manager.send(this, _manager.tag_exp_session);
	}
	
	@Override
    public void recycle() {
		super.recycle();
	}
	
	@Override
    public void addSessionListener(SessionListener listener) {
		super.addSessionListener(listener);

		_manager.send(this, _manager.tag_add_session);
    }
	
	@Override
    public void setPrincipal(Principal principal) {
		super.setPrincipal(principal);
		
		_manager.send(this, _manager.tag_add_session);
	}

}
