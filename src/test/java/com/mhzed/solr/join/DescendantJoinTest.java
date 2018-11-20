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

/**
 * This test simulates a file-system, the directory structure is stored in a separate 'shadow'
 * collection, where as the documents are stored in a normal Solr cloud collection.  
 * The modification of directory structure will not affect documents, and folder descendant filtering 
 * is achieved via solr's 'graph' and 'join' query.
 * 
 * @author minhongz@gmail.com
 *
 */
public class DescendantJoinTest extends SolrCloudTestCase {
	static CloudSolrClient client;
	static final int NodeCount = 5;		// How many solr nodes to create
	
	static final String DocCollection = "docs";
	static final String FolderCollection = "folders";
	
	static final String PathField = "path_descendent_path";
	static final String IdField = "id";	// for test join by string
	static final String ParentField = "parent_id_s";
	static {
		System.setProperty("java.security.egd", "file:/dev/./urandom");		
		System.setProperty("solr.log.dir", "./logs");
	}
	
	/**
	 * In solr cloud, create a shadowCollection on the same nodes as originalCollection,
	 * always with 1 shard and N replicas, where N = number of nodes hosting the originalCollection
	 *   
	 * @param client
	 * @param originalCollection
	 * @param shadowCollection
	 * @param config the name of config set to be use for newCollection
	 * @return the collection create request to be processed
	 * @throws IOException
	 */
	public static CollectionAdminRequest.Create shadowCreate(
					CloudSolrClient client, String originalCollection,
					String shadowCollection, String config) throws IOException {
		Collection<String> nodeset = client.getClusterStateProvider().getCollection(originalCollection).getSlices()
						.stream().flatMap((slice)-> slice.getReplicas().stream()).map((replica)->
						replica.getNodeName()).distinct().collect(Collectors.toList()); 						
		CollectionAdminRequest.Create req = CollectionAdminRequest.createCollection(
						shadowCollection, config, 1, nodeset.size());
		req.setCreateNodeSet(nodeset.stream().collect(Collectors.joining(",")));
		return req;
	}	
	
	@BeforeClass
	public static void setup() throws Exception {
		Builder builder = configureCluster(NodeCount);
		Path p = new File(DescendantJoinTest.class.getResource("../../../../test_core/conf").toURI()).toPath();
		builder.addConfig("_default", p);
    builder.configure();
		cluster.waitForAllNodes(60000);
		client = cluster.getSolrClient();
		
		// create DocCollection on subset of all nodes (NodeCount -2), for testing shadowCreate.
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
		r = client.query(DocCollection, pathJoinQuery("*:*", "/0"));
		assertEquals(13, r.getResults().size());
		
		r = client.query(DocCollection, graphJoinQuery("*:*", "3"));
		assertEquals(4, r.getResults().size());		
		r = client.query(DocCollection, pathJoinQuery("*:*", "/0/0"));
		assertEquals(4, r.getResults().size());		
	}
	
	/**
	 * Folder descendant filtering via graph query: traversing descendants by exploiting the
	 * ParentField in each folder object.
	 * 
	 * @param mainQuery is the DocCollection search query
	 * @param folderId of folder to filter under
	 * @return the search query
	 */
	SolrQuery graphJoinQuery(String mainQuery, String folderId) {
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
						"{!join fromIndex=%s from=%s to=%s}{!graph from=%s to=%s}%s:%s", 
						FolderCollection, IdField, "folder_id_s",
						ParentField, IdField, IdField, ClientUtils.escapeQueryChars(folderId))).setRows(1000000);
	}
	/**
	 * Folder descendant filtering via descendant_path fieldType: find descendants by exploiting the
	 * PathField in each folder object.
	 * 
	 * @param mainQuery is the DocCollection search query
	 * @param path of folder to filter under
	 * @return the search query
	 */
	
	SolrQuery pathJoinQuery(String mainQuery, String path) {
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
						"{!join fromIndex=%s from=%s to=%s}%s:%s", 
						FolderCollection, IdField, "folder_id_s",
						PathField, ClientUtils.escapeQueryChars(path))).setRows(1000000);
	}
			
	/**
	 * Generate a branch of folders using the supplied parameters.
	 * 
	 * @param path parent path
	 * @param parentId parent folder id
	 * @param idoffset offset of id of folders to be generated
	 * @param width how many direct sub folders to generate
	 * @param depth how many level of descendant folders to generate
	 * @return A collection of all folders generated
	 */
	List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
			docs.add(docOf(IdField, String.valueOf(id), PathField, path + "/" + w, ParentField, parentId));
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
			docOf("folder_" + IdField + "_s", folder.getFieldValue(IdField))
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
