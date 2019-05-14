package insomnia.qrewriting.database.driver.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

import insomnia.json.Element;
import insomnia.json.ElementArray;
import insomnia.json.ElementLiteral;
import insomnia.json.ElementMultiple;
import insomnia.json.ElementNumber;
import insomnia.json.ElementObject;
import insomnia.json.ElementString;
import insomnia.json.Json;
import insomnia.json.JsonBuilder;
import insomnia.json.JsonBuilderException;
import insomnia.qrewriting.context.Context;
import insomnia.qrewriting.query.Label;
import insomnia.qrewriting.query.Query;
import insomnia.qrewriting.query.node.Node;
import insomnia.qrewriting.query.node.NodeValue;
import insomnia.qrewriting.query.node.NodeValueExists;
import insomnia.qrewriting.query.node.NodeValueLiteral;
import insomnia.qrewriting.query.node.NodeValueNumber;
import insomnia.qrewriting.query.node.NodeValuePhantom;
import insomnia.qrewriting.query.node.NodeValueString;

public class JsonBuilder_query extends JsonBuilder
{
	Context context;
	Query   query;

	public JsonBuilder_query(Context context)
	{
		super();
		setContext(context);
	}

	public JsonBuilder_query(Context context, Json j)
	{
		super(j);
		setContext(context);
	}

	public JsonBuilder_query(Context context, Json j, Query q)
	{
		super(j);
		setContext(context);
		setQuery(q);
		setBuilded(j);
	}

	public void setQuery(Query query)
	{
		this.query = query;
	}

	private void setContext(Context context)
	{
		this.context = context;
	}

	@Override
	public void build() throws JsonBuilderException
	{
		Json json = getBuilded();

		if (query.getRoot().isLeaf())
		{
			throw new JsonBuilderException("Bad query structure for " + query);
		}
		ElementObject root = makeJson(query.getRoot());

		if (root.getObject().size() == 1 && root.getObject().containsKey("$or") && root.getObject().get("$or").isArray())
		{
			ElementArray or = (ElementArray) root.getObject().get("$or");

			for (Element orChild : or.getArray())
			{
				if (orChild.isObject())
					unwindElementMultiple((ElementObject) orChild);
			}
		}
		else
			unwindElementMultiple(root);

		json.setDocument(root);
	}

	private ElementObject makeJson(Node node) throws JsonBuilderException
	{
		ElementObject ret = new ElementObject();

		for (Node ncur : node)
		{
			NodeValue vcur = ncur.getValue();
			Label     lcur = ncur.getLabel();

			if (ncur.isLeaf())
			{
				Element newVal;

				if (vcur instanceof NodeValueNumber)
				{
					newVal = new ElementNumber(((NodeValueNumber) vcur).getNumber());
				}
				else if (vcur instanceof NodeValueString)
				{
					newVal = new ElementString(((NodeValueString) vcur).getString());
				}
				else if (vcur instanceof NodeValueLiteral)
				{
					newVal = new ElementLiteral(((NodeValueLiteral) vcur).getLiteral());
				}
				else if (vcur instanceof NodeValueExists)
				{
					newVal = new ElementObject();
					newVal.add(new ElementLiteral(ElementLiteral.Literal.TRUE), "$exists");
				}
				else if (vcur instanceof NodeValuePhantom)
				{
					newVal = null;
				}
				else
				{
					throw new JsonBuilderException("Cannot make value " + vcur + " of " + ncur);
				}
				ret.add(newVal, lcur.get());
			}
			else
			{
				ElementObject newVal    = makeJson(ncur);
				ElementObject elemMatch = new ElementObject();
				elemMatch.add(newVal, "$elemMatch");
				ret.add(elemMatch, lcur.get());
			}
		}
		return ret;
	}

	private void unwindElementMultiple(ElementObject elementObject)
	{
		List<Pair<String,ElementMultiple>> elementMultiples = new ArrayList<>();
		
		for (Entry<String, ? extends Element> child : elementObject.getObject().entrySet())
		{
			Element echild = child.getValue();

			if (echild.isObject())
				unwindElementMultiple((ElementObject) echild);
			else if (echild instanceof ElementMultiple)
			{
				elementMultiples.add(Pair.of(child.getKey(), (ElementMultiple)child.getValue()));
			}
		}
		
		for(Pair<String, ElementMultiple> pair : elementMultiples)
		{
			ElementArray eachild = (ElementArray) pair.getValue();
			ElementArray array   = new ElementArray();
			elementObject.add(array, "$and");
			elementObject.remove(pair.getKey());

			for (Element childchild : eachild.getArray())
			{
				ElementObject tmp = new ElementObject();
				tmp.add(childchild, pair.getKey());
				array.add(tmp);
			}
		}
	}

	@Override
	public Json newBuild() throws JsonBuilderException
	{
		Json json = new Json();
		setBuilded(json);
		build();
		return json;
	}
}
