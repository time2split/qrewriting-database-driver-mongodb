package insomnia.qrewriting.database.driver.mongodb;

import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.output.WriterOutputStream;
import org.apache.commons.lang3.NotImplementedException;

import insomnia.json.Json;
import insomnia.json.JsonWriter;
import insomnia.qrewriting.database.driver.internal.JsonBuilder_query;
import insomnia.qrewriting.query.Label;
import insomnia.qrewriting.query.Query;
import insomnia.qrewriting.query.node.Node;
import insomnia.qrewriting.query.node.NodeValue;
import insomnia.qrewriting.query.node.NodeValueExists;
import insomnia.qrewriting.query.node.NodeValueFantom;
import insomnia.qrewriting.query.node.NodeValueLiteral;
import insomnia.qrewriting.query.node.NodeValueString;

public class MyQueryManager
		extends insomnia.qrewriting.database.driver.internal.MyQueryManager
{

	public MyQueryManager()
	{
		super();
	}

	public MyQueryManager(Query... queries)
	{
		super(queries);
	}

	public MyQueryManager(Collection<Query> queries)
	{
		super(queries);
	}

	@Override
	public void writeStrFormat(Writer writer, Query query) throws Exception
	{
		OutputStream buffer = new WriterOutputStream(writer,
			Charset.defaultCharset());

		try (JsonWriter jsbuffer = new JsonWriter(buffer);)
		{
			JsonBuilder_query jsbuilder = new JsonBuilder_query();

			{
				boolean compact = getDriver().getOption("json.prettyPrint")
						.equals("false");
				jsbuffer.getOptions().setCompact(compact);
			}
			Json document;
			Query dq = DQueryFromQuery(query);

			jsbuilder.setQuery(dq);
			document = jsbuilder.newBuild();

			if (dq.childsArePaths())
				buffer.write("find(".getBytes());
			else
				buffer.write("aggregate(".getBytes());

			jsbuffer.write(document);
			buffer.write(')');
		}
		catch (Exception e)
		{
			throw e;
		}
	}

	public Query DQueryFromQuery(Query query)
	{
		if (query.childsArePaths())
		{
			Node[] childs = new Node[query.getNbOfChilds()];
			int i = 0;

			for (Node child : query)
				childs[i++] = DNodePath(child);

			return new Query().addChildMe(childs);
		}
		return DQueryTree(query);
	}

	/**
	 * Construit un noeud si le node est un noeud opération (ex: $exists)
	 * 
	 * @param node
	 * @return
	 */
	private Node DNodeOperation(Node node)
	{
		final NodeValue val = node.getValue();

		if (val instanceof NodeValueExists)
			return new Node().setLabelMe("$exists")
					.setValueMe(new NodeValueLiteral("true"));

		return null;
	}

	/**
	 * Construit un noeud unique à partir du chemin $node (notation pointée)
	 * 
	 * @param node
	 * @return
	 */
	private Node DNodePath(Node node)
	{
		ArrayList<String> buffer = new ArrayList<>(node.getNbOfDescendants());
		Node extra;

		for (;;)
		{
			extra = DNodeOperation(node);
			buffer.add(node.getLabel().get());

			if (extra != null)
			{
				// On supprime le noeud du buffer
				buffer.remove(buffer.size() - 1);
				break;
			}
			else if (node.isLeaf())
			{
				break;
			}
			node = node.getChilds().getChilds()[0];
		}
		Node ret = new Node().setLabelMe(String.join(".", buffer))
				.setValueMe(node.getValue());

		if (extra != null)
			ret.addChild(extra);

		return ret;
	}

	private Query DQueryTree(Query root)
	{
		Query ret = new Query();
		Node[] trees = root.getTrees();
		Node[] stages = new Node[trees.length + 1];
		int i = 0;

		for (Node child : trees)
		{
			List<String> buffer = child.backCollect(n -> n.getLabel().get());
			String label = String.join(".", buffer);
			stages[i++] = mongoOp_addFields(label);
		}
		stages[i++] = DLastStage(root, "$match");

		for (Node stage : stages)
			ret.addChild(new Node().addChildMe(stage));

		return ret;
	}

	private Node DLastStage(Node node, String mongoMatchOp)
	{
		Node[] childs = new Node[node.getNbOfChilds()];
		Node tmp = new Node().setLabelMe(mongoMatchOp);
		Label label = node.getLabel();
		Node ret;
		Node leaf;

		if (label != null)
		{
			ret = new Node();
			ret.setLabel(new Label(label));
			ret.addChild(tmp);
			leaf = tmp;
		}
		else
		{
			ret = tmp;
			leaf = tmp;
		}

		int i = 0;

		for (Node child : node)
		{
			if (child.isPath())
				tmp = DNodePath(child);
			else
				tmp = DLastStage(child, "$elemMatch");

			childs[i++] = tmp;
		}
		leaf.addChildMe(childs);
		return ret;
	}

	private Node mongoOp_addFields(String label)
	{
		final String var = "$" + label;
		Node ret = new Node().setLabelMe("$addFields");

		Node nif = new Node().setLabelMe("if");
		Node nthen = new Node().setLabelMe("then");
		Node nelse = new Node().setLabelMe("else");

		nif.newChildHim().setLabelMe("$isArray")
				.setValue(new NodeValueString(var));

		nthen.setValue(new NodeValueString(var));

		nelse.addChild(new Node().setValueMe(new NodeValueString(var)),
			new Node().setValueMe(NodeValueFantom.getInstance()));

		ret.newChildHim().setLabelMe(label).newChildHim().setLabelMe("$cond")
				.addChild(nif, nthen, nelse);
		;
		return ret;
	}

	@Override
	public Query merge(Query... queries)
	{
		throw new NotImplementedException(
			this.getClass().getName() + " does not implement merge()");
	}

	@Override
	public boolean canMerge(Query... queries)
	{
		for (Query q : queries)
		{
			if (!q.childsArePaths())
				return false;
		}
		return true;
	}

}
