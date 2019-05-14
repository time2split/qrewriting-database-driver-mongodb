package insomnia.qrewriting.database.driver.mongodb;

import org.apache.commons.lang3.NotImplementedException;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import insomnia.qrewriting.database.AbstractDriver;
import insomnia.qrewriting.database.DriverException;
import insomnia.qrewriting.database.driver.DriverQueryBuilder;
import insomnia.qrewriting.database.driver.DriverQueryEvaluator;
import insomnia.qrewriting.database.driver.DriverQueryManager;

public class TheDriver extends AbstractDriver
{
	private MongoClient client;

	@Override
	public DriverQueryBuilder getAQueryBuilder() throws DriverException
	{
		throw new DriverException(new NotImplementedException(""));
	}

	@Override
	public DriverQueryManager getAQueryManager() throws DriverException
	{
		return new MyQueryManager(this);
	}

	@Override
	public DriverQueryEvaluator getAQueryEvaluator() throws DriverException
	{
		return new MyQueryEvaluator(this);
	}

	@Override
	public void unload()
	{
		if (client != null)
			client.close();
	}

	MongoClient getClient()
	{
		if (client != null)
			return client;

		client = MongoClients.create();
		return client;
	}
}
