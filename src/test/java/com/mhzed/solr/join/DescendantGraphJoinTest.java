package com.mhzed.solr.join;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DescendantGraphJoinTest extends SolrCloudTestCase {
	static CloudSolrClient client;
	static final int NodeCount = 5;
	
	static final String DocCollection = "docs";
	static final String FolderCollection = "folders";
	
	static final String PathField = "path_descendent_path";
	static final String IdField = "id";	// for test join by string
	static final String IntField = "id_i";	// for test join by integer
	static final String LongField = "id_l";	// for test join by long
	static final String ParentField = "parent_id_s";
	static {
		System.setProperty("java.security.egd", "file:/dev/./urandom");		
		System.setProperty("solr.log.dir", "./logs");
	}
	
	/**
	 * In solr cloud, create a newCollection on the same nodes as originalCollection,
	 * always with 1 shard and N replicas
	 * 
	 * TODO: when a document shard is splitted, the node set may increase, in which case
	 * of stickily created collection should add a replica(s) on the new node(s).
	 *  
	 * @param client
	 * @param originalCollection
	 * @param newCollection
	 * @param config
	 * @return
	 * @throws IOException
	 */
	public static CollectionAdminRequest.Create shadowCreate(
					CloudSolrClient client, String originalCollection,
					String newCollection, String config) throws IOException {
		Collection<String> nodeset = client.getClusterStateProvider().getCollection(originalCollection).getSlices()
						.stream().flatMap((slice)-> slice.getReplicas().stream()).map((replica)->
						replica.getNodeName()).distinct().collect(Collectors.toList()); 						
		CollectionAdminRequest.Create req = CollectionAdminRequest.createCollection(
						newCollection, config, 1, nodeset.size());
		req.setCreateNodeSet(nodeset.stream().collect(Collectors.joining(",")));
		return req;
	}	
	
	@BeforeClass
	public static void setup() throws Exception {
		Builder builder = configureCluster(NodeCount);
		
		Path p = new File(DescendantGraphJoinTest.class.getResource("../../../../test_core/conf").toURI()).toPath();
		builder.addConfig("_default", p);
    builder.configure();
		cluster.waitForAllNodes(60000);
		client = cluster.getSolrClient();
		
		CollectionAdminRequest.createCollection(
						DocCollection, "_default", NodeCount - 2 , 1).process(client);
		shadowCreate(client, DocCollection, FolderCollection, "_default").process(client);
	}
  @AfterClass
  public static void teardown() throws Exception {
  }	
    
	@Test
	public void test() throws Exception {
		List<SolrInputDocument> folders = branch("", null, 0, 3, 3);	// size: 3^1 + 3^2 + 3^3 = 39
		
		new UpdateRequest().add(folders).process(client, FolderCollection);
		client.commit(FolderCollection);
		new UpdateRequest().add(docs(folders)).process(client, DocCollection);
		client.commit(DocCollection);
	
		QueryResponse r;
		r = client.query(DocCollection, graphJoinQuery("*:*", "0"));
		assertEquals(13, r.getResults().size());
		
		r = client.query(DocCollection, graphJoinQuery("*:*", "3"));
		assertEquals(4, r.getResults().size());		
	}
	
	SolrQuery graphJoinQuery(String mainQuery, String id) {
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
						"{!join fromIndex=%s from=%s to=%s}{!graph from=%s to=%s}%s:%s", 
						FolderCollection, "id", "folder_id_s",
						ParentField, IdField, IdField, ClientUtils.escapeQueryChars(id))).setRows(10000);
	}
			
	// generate test folder docs
	List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
			docs.add(docOf(IdField, String.valueOf(id), IntField, id, LongField, id, 
							PathField, path + "/" + w, ParentField, parentId));
			childrenIds.add(id);
			id++;
		}
		for (int w=0; w<width; w++) {
			List<SolrInputDocument> b = branch(path + "/" + w, childrenIds.get(w), id, width, depth-1);
			id += b.size();
			docs.addAll(b);
		}
		return docs;
	}
	// generate 1 doc for each folder 
	private List<SolrInputDocument> docs(List<SolrInputDocument> folders) {
		return folders.stream().map(folder->
			docOf("folder_" + IdField + "_s", folder.getFieldValue(IdField), 
							"folder_" + IntField, folder.getFieldValue(IntField),
							"folder_" + LongField, folder.getFieldValue(LongField))
		).collect(Collectors.toList());
		
	}
	
	private static SolrInputDocument docOf(Object... args) {
		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < args.length; i += 2) {
			doc.addField(args[i].toString(), args[i + 1]);
		}
		return doc;
	}
		
}
