package com.findwise.hydra;

import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class TestModule extends AbstractModule {
	private String namespace;
	private DatabaseConfiguration config;
	
	public TestModule() {
		this("junitspace");
	}
	
	public TestModule(String namespace) {
		this(namespace, null);
	}
	
	public TestModule(String namespace, DatabaseConfiguration config) {
		this.namespace = namespace;
		this.config = config;
	}

	@Override
	protected void configure() {
		bind(DatabaseConnector.class).to(MongoConnector.class);
	}
	
	@Provides @Singleton
	protected DatabaseConfiguration getConfiguration() {
		if(config!=null) {
			return config;
		}
		return new DatabaseConfiguration() {
			
			@Override
			public String getNamespace() {
				return namespace;
			}
			
			@Override
			public String getDatabaseUrl() {
				return "127.0.0.1";
			}
			
			@Override
			public String getDatabaseUser() {
				return "admin";
			}
			
			@Override
			public String getDatabasePassword() {
				return "changeme";
			}

			@Override
			public int getOldMaxSize() {
				return 10;
			}

			@Override
			public int getOldMaxCount() {
				return 1000;
			}
		};
	}
}
