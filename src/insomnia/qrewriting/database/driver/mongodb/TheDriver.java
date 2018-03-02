package insomnia.qrewriting.database.driver.mongodb;

import insomnia.qrewriting.database.Driver;
import insomnia.qrewriting.database.driver.DriverQueryBuilder;
import insomnia.qrewriting.database.driver.DriverQueryManager;;

public class TheDriver extends Driver
{

	@Override
	public Class<? extends DriverQueryBuilder> getQueryBuilderClass()
	{
		return null;
	}

	@Override
	public Class<? extends DriverQueryManager> getQueryManagerClass()
	{
		return MyQueryManager.class;
	}

}
