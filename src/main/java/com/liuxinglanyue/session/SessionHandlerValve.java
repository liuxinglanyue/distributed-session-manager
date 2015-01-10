package com.liuxinglanyue.session;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

public class SessionHandlerValve extends ValveBase {
	private AbstractSessionManager sessionManager;
	
	public void setSessionManager(AbstractSessionManager sessionManager) {
		this.sessionManager = sessionManager;
	}

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		try {
			this.getNext().invoke(request, response);
		} finally {
			sessionManager.afterRequest();
		}
	}

}
