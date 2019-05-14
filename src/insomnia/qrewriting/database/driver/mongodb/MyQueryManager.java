package insomnia.qrewriting.database.driver.mongodb;

import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.WriterOutputStream;

import insomnia.builder.BuilderException;
import insomnia.json.Json;
import insomnia.json.JsonWriter;
import insomnia.qrewriting.database.Driver;
import insomnia.qrewriting.query.DefaultQuery;
import insomnia.qrewriting.query.Label;
import insomnia.qrewriting.query.LabelFactory;
import insomnia.qrewriting.query.Query;
import insomnia.qrewriting.query.QueryBuilder;
import insomnia.qrewriting.query.node.Node;
import insomnia.qrewriting.query.node.NodeBuilder;
import insomnia.qrewriting.query.node.NodeValue;
import insomnia.qrewriting.query.node.NodeValueExists;
import insomnia.qrewriting.query.node.NodeValueLiteral;
import insomnia.qrewriting.query.node.NodeValuePhantom;
import insomnia.qrewriting.query.node.NodeValueString;

public class MyQueryManager extends insomnia.qrewriting.database.driver.internal.MyQueryManager
{
	public MyQueryManager(Driver driver)
	{
		super(driver);
	}

	@Override
	public void writeStrFormat(Writer writer, Query query) throws Exception
	{
		OutputStream buffer = new WriterOutputStream(writer, Charset.defaultCharset());

		try (JsonWriter jsbuffer = new JsonWriter(buffer);)
		{
			JsonBuilder_query jsbuilder = new JsonBuilder_query(getDriver().getContext());

			{
				boolean compact = getDriver().getOption("json.prettyPrint", "false").equals("false");
				jsbuffer.getOptions().setCompact(compact);
			}
			Json  document;
			Query dq = MongoQuerySimpleEMatch(query);

			jsbuilder.setQuery(dq);
			document = jsbuilder.newBuild();
//			buffer.write("find(".getBytes());
			jsbuffer.write(document);
//			buffer.write(')');
		}
		catch (Exception e)
		{
			throw e;
		}
	}

	// =========================================================================

	private Query MongoQuerySimpleEMatch(Query query) throws BuilderException
	{
		QueryBuilder qbuilder = new QueryBuilder(QueryBuilder.newQuery(query.getClass()));
		NodeBuilder  nbuilder = qbuilder.getRootNodeFactory();

		for (Node child : query.getRoot())
			MongoQuerySimpleEMatch_node(child, nbuilder);

		nbuilder.build();
		return qbuilder.getBuilded();
	}

	private void MongoQuerySimpleEMatch_node(Node node, NodeBuilder nbuilder) throws BuilderException
	{
		String pathPrefix;
		// Get the longuest path from node to childs
		{
			StringBuilder sbuilder = new StringBuilder();

			while (node.getNbOfChilds() == 1)
			{
				if (sbuilder.length() > 0)
					sbuilder.append('.');

				sbuilder.append(node.getLabel().get());
				node = node.getChilds().getChilds()[0];
			}

			if (sbuilder.length() > 0)
				sbuilder.append('.');

			sbuilder.append(node.getLabel().get());
			pathPrefix = sbuilder.toString();
		}
		LabelFactory lfactory = getDriver().getContext().getLabelFactory();
		NodeValue    nvalue   = node.getValue();
		nbuilder.child().setLabel(lfactory.from(pathPrefix)).setValue(nvalue);

		if (node.getNbOfChilds() > 0)
		{
			for (Node child : node)
				MongoQuerySimpleEMatch_node(child, nbuilder);
		}
		nbuilder.end();
	}

	// =========================================================================

	private Query DQueryFromQuery(Query query) throws BuilderException
	{
		Node root = query.getRoot();

		if (root.childsArePaths())
		{
			Node[] childs = new Node[root.getNbOfChilds()];
			int    i      = 0;

			for (Node child : root)
				childs[i++] = DNodePath(child);

			Query ret = new DefaultQuery();
			ret.addNode(false, childs);
			return ret;
		}
		return DQueryTree(query);
	}

	/**
	 * Construit un noeud si le node est un noeud opération (ex: $exists)
	 * 
	 * @param node
	 * @return
	 * @throws BuilderException
	 */
	private Node DNodeOperation(Node node) throws BuilderException
	{
		final NodeValue val = node.getValue();

		if (val instanceof NodeValueExists)
		{
			Node        ret      = NodeBuilder.newNode(node.getClass());
			NodeBuilder nbuilder = new NodeBuilder(ret);
			nbuilder.setLabel(getDriver().getContext().getLabelFactory().from("$exists")) //
				.setValue(new NodeValueLiteral("true")) //
				.build();
			return ret;
		}
		return null;
	}

	/**
	 * Construit un noeud unique à partir du chemin $node (notation pointée)
	 * 
	 * @param node
	 * @return
	 * @throws BuilderException
	 */
	private Node DNodePath(Node node) throws BuilderException
	{
		ArrayList<String> buffer = new ArrayList<>(node.getNbOfDescendants());
		Node              extra;

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
		Node        ret      = NodeBuilder.newNode(node.getClass());
		NodeBuilder nbuilder = new NodeBuilder(ret);

		nbuilder.setLabel(getDriver().getContext().getLabelFactory().from(String.join(".", buffer))) //
			.setValue(node.getValue()) //
			.build();

		if (extra != null)
			nbuilder.addChild(false, extra);

		nbuilder.build();
		return ret;
	}

	private Query DQueryTree(Query query) throws BuilderException
	{
		Node   root   = query.getRoot();
		Query  ret    = QueryBuilder.newQuery(query.getClass());
		Node[] trees  = root.getTrees();
		Node[] stages = new Node[trees.length + 1];
		int    i      = 0;

		for (Node child : trees)
		{
			List<String> buffer = child.backCollect(n -> n.getLabel().get());
			String       label  = String.join(".", buffer);
			stages[i++] = mongoOp_addFields(label, root.getClass());
		}
		stages[i++] = DLastStage(root, "$match");
		NodeBuilder nbuilder = new NodeBuilder(ret.getRoot());

		for (Node stage : stages)
			nbuilder.child().addChild(false, stage).end();

		nbuilder.build();
		return ret;
	}

	private Node DLastStage(Node node, String mongoMatchOp) throws BuilderException
	{
		Node[] childs = new Node[node.getNbOfChilds()];
		Node   tmp    = NodeBuilder.newNode(node.getClass());
		Label  label  = node.getLabel();
		Node   ret;
		Node   leaf;

		tmp.setLabel(getDriver().getContext().getLabelFactory().from(mongoMatchOp));

		if (label != null)
		{
			ret = NodeBuilder.newNode(node.getClass());
			ret.setLabel(getDriver().getContext().getLabelFactory().from(label));
			ret.addChild(false, tmp);
			leaf = tmp;
		}
		else
		{
			ret  = tmp;
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
		leaf.addChild(false, childs);
		return ret;
	}

	private Node mongoOp_addFields(String label, Class<? extends Node> nodeClass) throws BuilderException
	{
		final String var          = "$" + label;
		LabelFactory labelFactory = getDriver().getContext().getLabelFactory();
		Node         ret          = NodeBuilder.newNode(nodeClass);
		ret.setLabel(labelFactory.from("$addFields"));

		Node nif   = NodeBuilder.newNode(nodeClass);
		Node nthen = NodeBuilder.newNode(nodeClass);
		Node nelse = NodeBuilder.newNode(nodeClass);

		nif.setLabel(labelFactory.from("if"));
		nthen.setLabel(labelFactory.from("then"));
		nelse.setLabel(labelFactory.from("else"));

		nthen.setValue(new NodeValueString(var));

		NodeBuilder nbuilder = new NodeBuilder(nodeClass);

		nbuilder.setBuilded(nif);
		nbuilder.child().setLabel(labelFactory.from("$isArray")).setValue(new NodeValueString(var));
		nbuilder.build();

		nbuilder.setBuilded(nelse);
		nbuilder.child().setValue(new NodeValueString(var)).end();
		nbuilder.child().setValue(NodeValuePhantom.getInstance()).end();
		nbuilder.build();

		nbuilder.setBuilded(ret);
		nbuilder.child().setLabel(labelFactory.from(label)) //
			.child().setLabel(labelFactory.from("$cond")) //
			.addChild(false, nif, nthen, nelse);
		nbuilder.build();

		return ret;
	}

	@Override
	public Query merge(Query... queries)
	{
		try
		{
			QueryBuilder qbuilder = new QueryBuilder(queries[0].getClass());
			qbuilder.setBuilded(QueryBuilder.newQuery(queries[0].getClass()));

			NodeBuilder nbuilder = qbuilder.getRootNodeFactory();
			Label       orLabel  = getDriver().getContext().getLabelFactory().from("$or");

			for (Query query : queries)
				nbuilder.child().setLabel(orLabel).addChild(false, query.getRoot().getChilds().getChilds()).end();

			return qbuilder.getBuilded();
		}
		catch (BuilderException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean canMerge(Query... queries)
	{
		return true;
	}
}
