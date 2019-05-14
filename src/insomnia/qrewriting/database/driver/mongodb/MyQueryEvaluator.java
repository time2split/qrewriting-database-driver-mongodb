package insomnia.qrewriting.database.driver.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import insomnia.qrewriting.database.Driver;
import insomnia.qrewriting.database.DriverException;
import insomnia.qrewriting.database.driver.CursorAggregation;
import insomnia.qrewriting.database.driver.DriverQueryEvaluator;
import insomnia.qrewriting.query.Query;
import insomnia.qrewriting.query.QueryManager;

class MyQueryEvaluator implements DriverQueryEvaluator
{
	TheDriver driver;
	int       blockSize;

	public MyQueryEvaluator(Driver driver)
	{
		blockSize = Integer.parseInt(driver.getOption("sys.evaluation.block", "0").toString());

		this.driver = (TheDriver) driver;
	}

	@Override
	public TheDriver getDriver()
	{
		return driver;
	}

	@Override
	public Cursor evaluate(Query... queries) throws DriverException
	{
		int                nbBlocks = queries.length / blockSize;
		int                rest     = queries.length % blockSize;
		Collection<Cursor> cursors  = new ArrayList<>(nbBlocks);
		int                offset   = 0;

		for (int i = 0; i < nbBlocks; i++)
		{
			Cursor cursor = evaluateABlock(Arrays.copyOfRange(queries, offset, offset + blockSize));

			if (cursor != null && cursor.size() > 0)
				cursors.add(cursor);

			offset += blockSize;
		}

		if (rest > 0)
		{
			Cursor cursor = evaluateABlock(Arrays.copyOfRange(queries, offset, offset + rest));

			if (cursor != null && cursor.size() > 0)
				cursors.add(cursor);
		}
		return new CursorAggregation(cursors);
	}

	private Cursor evaluateABlock(Query[] queries) throws DriverException
	{
		QueryManager qmanager = driver.getAQueryManager();
		String       query_s;

		try
		{
			if (queries.length > 1)
			{
				StringBuilder sbuilder = new StringBuilder();

				for (Query q : queries)
				{

					if (sbuilder.length() > 0)
						sbuilder.append(',');

					sbuilder.append(qmanager.getStrFormat(q));
				}
				sbuilder.insert(0, "{$or: [");
				sbuilder.append("]}");
				query_s = sbuilder.toString();
			}
			else
			{
				query_s = qmanager.getStrFormat(queries[0]);
			}
		}
		catch (Exception e)
		{
			throw new DriverException(e);
		}
		MongoClient               client     = driver.getClient();
		MongoCollection<Document> collection = client.getDatabase((String) driver.getOption("db.name", "")).getCollection((String) driver.getOption("collection.name", ""));
		Document                  filter     = Document.parse(query_s);
		long                      size       = collection.countDocuments(filter);

		if (size == 0)
			return null;

		FindIterable<Document>     res = collection.find(filter);
		FindIterable2CursorAdapter tmp = new FindIterable2CursorAdapter(size, res);
		return tmp;
	}

	private class FindIterable2CursorAdapter implements Cursor
	{
		FindIterable<Document> mongoResult;
		long                   size = -1;

		public FindIterable2CursorAdapter(long size, FindIterable<Document> it)
		{
			this.size   = size;
			mongoResult = it.batchSize(blockSize);
		}

		@Override
		public Iterator<Query> iterator()
		{
			MongoCursor<Document> it = mongoResult.iterator();

			return new Iterator<Query>()
			{
				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Query next()
				{
					// TODO
					return null;
				}
			};
		}

		@Override
		public long size()
		{
			if (size != -1)
				return size;

			MongoCursor<Document> local_it = mongoResult.iterator();

			size = 0;

			try (local_it)
			{
				while (local_it.hasNext())
				{
					local_it.next();
					size++;
				}
			}
			return size;
		}
	}
}
