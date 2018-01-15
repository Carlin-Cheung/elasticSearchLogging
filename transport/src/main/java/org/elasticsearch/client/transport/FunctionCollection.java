package org.elasticsearch.client.transport;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.HdrHistogram.Histogram;
import org.apache.lucene.index.Terms;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Hello world!
 *
 */
public class FunctionCollection 
{
	
	public static IndexResponse createIndexResponse(TransportClient client) throws IOException {
		IndexResponse response = client.prepareIndex("twitter", "tweet")
				 .setSource(jsonBuilder()
						     .startObject()
						     .field("user", "zhanggr")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch-1")
		                    .endObject()
		                    ).get();
		return response;
	}
	
	
	public static GetResponse createGetResponse(TransportClient client) throws IOException {
		GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
		return response;
	}
	
	public static DeleteResponse createDeleteResponse(TransportClient client) throws IOException {
		DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
		return response;
	}
	
	/**
	 * delete a given set of documents based on the result of a query
	 * @param client
	 * @return
	 * @throws IOException
	 */
	public static void createBulkByScrollResponse(TransportClient client) throws IOException {
		/*BulkByScrollResponse response =
			    DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
			        .filter(QueryBuilders.matchQuery("gender", "male")) 
			        .source("persons")                                  
			        .get();                                             

			long deleted = response.getDeleted();  
		return deleted;
		*/	
		DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
	    .filter(QueryBuilders.matchQuery("gender", "male"))                  
	    .source("persons")                                                   
	    .execute(new ActionListener<BulkByScrollResponse>() {           
	        @Override
	        public void onResponse(BulkByScrollResponse response) {
	            long deleted = response.getDeleted();                        
	        }
	        @Override
	        public void onFailure(Exception e) {
	            // Handle the exception
	        }
	    });
	}
	
	public static UpdateResponse createUpdateRequest(TransportClient client) throws IOException, InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("index");
		updateRequest.type("type");
		updateRequest.id("1");
		updateRequest.doc(jsonBuilder()
		        .startObject()
		            .field("gender", "male")
		        .endObject());
		
		return client.update(updateRequest).get();
	}
	
	/**
	 * The multi get API allows to get a list of documents based on their index, type and id
	 * @param client
	 * @throws IOException
	 */
	public static void createMultiGetResponse(TransportClient client) throws IOException {
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
			    .add("twitter", "tweet", "1")           
			    .add("twitter", "tweet", "2", "3", "4") 
			    .add("another", "type", "foo")          
			    .get();

			for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
			    GetResponse response = itemResponse.getResponse();
			    if (response.isExists()) {                      
			        String json = response.getSourceAsString(); 
			        System.out.println(json);
			    }
			}
	}
	
	/**
	 * The bulk API allows one to index and delete several documents in a single request
	 * @param client
	 * @throws IOException
	 */
	public static void createBulkRequest(TransportClient client) throws IOException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		// either use client#prepare, or use Requests# to directly build index/delete requests
		bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch")
		                    .endObject()
		                  )
		        );
		bulkRequest.add(client.prepareIndex("twitter", "tweet", "3")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "another post")
		                    .endObject()
		                  )
		        );
		
		BulkResponse bulkResponse = bulkRequest.get();
		if(bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
		}
	}
	
	/**
	 * The BulkProcessor class offers a simple interface to flush bulk operations 
	 * automatically based on the number or size of requests, or after a given period
	 * @param client
	 * @throws IOException
	 */
	public static void createBulkProcessor(TransportClient client) throws IOException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		BulkProcessor bulkProcessor = BulkProcessor.builder(
		        client,  
		        new BulkProcessor.Listener() {
		            @Override
		            public void beforeBulk(long executionId,
		                                   BulkRequest request) {  } 

		            @Override
		            public void afterBulk(long executionId,
		                                  BulkRequest request,
		                                  BulkResponse response) {  } 

		            @Override
		            public void afterBulk(long executionId,
		                                  BulkRequest request,
		                                  Throwable failure) { } 
		        })
		        .setBulkActions(10000) 
		        .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) 
		        .setFlushInterval(TimeValue.timeValueSeconds(5)) 
		        .setConcurrentRequests(1) 
		        .setBackoffPolicy(
		            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) 
		        .build();
		//bulkProcessor.add(new IndexRequest("twitter", "tweet", "5").source(/* your doc here */));
		bulkProcessor.add(new DeleteRequest("twitter", "tweet", "6"));
		
		bulkProcessor.flush();
		bulkProcessor.close();
	}
	
	/**
	 * The search API allows one to execute a search query and get back search hits that match the query. 
	 * It can be executed across one or more indices and across one or more types. 
	 * The query can be provided using the query Java API.
	 * @param client
	 * @throws IOException
	 */
	public static void createSearchResponse(TransportClient client) {
		
		QueryBuilder query = QueryBuilders.termQuery("kubernetes.host", "lab4");
//		QueryBuilder query = multiMatchQuery("lab4", "kubernetes.host"); // one test => multi field!
		
		SearchResponse response = client.prepareSearch("logstash-*")
		        .setTypes("fluentd")
		        .addSort("@timestamp", SortOrder.DESC)
		        .setScroll(new TimeValue(60000))
		        .setQuery(query)                 // Query
		        .setPostFilter(QueryBuilders.rangeQuery("@timestamp").from("0").to("1515814891000"))     // Filter: accord to the time sort
		        .setSize(100).setExplain(true)
		        .get();
		do {
			for(SearchHit hit : response.getHits().getHits()) {
				System.out.println(hit.getSourceAsString());
			}
			
			response = client.prepareSearchScroll(response.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();
		}while(response.getHits().getHits().length != 0);
	}
	
	/**
	 * The multi search API allows to execute several search requests within the same API. 
	 * @param client
	 */
	public static void createMultiSearch(TransportClient client) {
		SearchRequestBuilder srb1 = client.prepareSearch()
				.setQuery(QueryBuilders.queryStringQuery("out")).setSize(10);
		SearchRequestBuilder srb2 = client.prepareSearch().
				setQuery(QueryBuilders.matchQuery("user", "kimchy")).setSize(10);
		
		MultiSearchResponse sr = client.prepareMultiSearch()
				.add(srb1)
				.add(srb2)
				.get();
		
		// You will get all individual responses from MultiSearchResponse#getResponses()
		long nbHits = 0;
		for(MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			nbHits += response.getHits().getTotalHits();
		}
		System.out.println("the total hits : " + nbHits);
	}
	
	public static void createAggregationSearch(TransportClient client) {
		SearchResponse sr = client.prepareSearch()
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders.terms("agg1").field("field")
					)
				.addAggregation(
						AggregationBuilders.dateHistogram("agg2")
						.field("birth")
						.dateHistogramInterval(DateHistogramInterval.YEAR)
					)
				.get();
		Terms agg1 = sr.getAggregations().get("agg1");
		Histogram agg2 = sr.getAggregations().get("agg2");
		
	}
	
	/**
	 * term query
	 * @param client
	 */
	public static void createTermsSearchResponse(TransportClient client)  {
		
		QueryBuilder query = termsQuery("log",    
		    "error", "Exception", "Warning");   
		
		SearchResponse response = client.prepareSearch("logstash-*")
		        .setTypes("fluentd")
		        .addSort("@timestamp", SortOrder.DESC)
		        .setScroll(new TimeValue(60000))
		        .setQuery(query)                 // Query
		        .setPostFilter(QueryBuilders.rangeQuery("@timestamp").from("0").to("1515840091000"))     // Filter: accord to the time sort
		        .setSize(100).setExplain(true)
		        .get();
		do {
			for(SearchHit hit : response.getHits().getHits()) {
				System.out.println(hit.getSourceAsString());
			}
			
			response = client.prepareSearchScroll(response.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();
		}while(response.getHits().getHits().length != 0);
	}
	
	/**
	 * A query which wraps another query, but executes it in filter context. 
	 * All matching documents are given the same “constant” _score.
	 * @param client
	 */
	public static void createConstantQuery(TransportClient client) {
		QueryBuilder query = constantScoreQuery(
		        termQuery("kubernetes.host","lab3")      
		    )
		    .boost(2.0f);  

		SearchResponse response = client.prepareSearch("logstash-*")
			        .setTypes("fluentd")
			        .addSort("@timestamp", SortOrder.DESC)
			        .setScroll(new TimeValue(60000))
			        .setQuery(query)                 // Query
			        .setPostFilter(QueryBuilders.rangeQuery("@timestamp").from("0").to("1515840091000"))     // Filter: accord to the time sort
			        .setSize(100).setExplain(true)
			        .get();
			do {
				for(SearchHit hit : response.getHits().getHits()) {
					System.out.println(hit.getSourceAsString());
				}
				
				response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(new TimeValue(60000)).execute().actionGet();
			}while(response.getHits().getHits().length != 0);
	}
	
	/**
	 * The default query for combining multiple leaf or compound query clauses, as must, should, must_not, or filter clauses. 
	 * The must and should clauses have their scores combined — the more matching clauses, the better 
	 * — while the must_not and filter clauses are executed in filter context.
	 * @param client
	 */
	public static void createBoolQuery(TransportClient client) {
		QueryBuilder qb = boolQuery()
			    .must(termQuery("kubernetes.host", "lab3"))
			    .must(matchQuery("kubernetes.pod_name", "iotdb-master-8npsb"))
			    .filter(QueryBuilders.rangeQuery("@timestamp").from("0").to("1515840091000"));
		
		SearchResponse response = client.prepareSearch("logstash-*")
		        .setTypes("fluentd")
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)                 // Query
		        .setSize(100).setExplain(true)
		        .get();
		System.out.println(response.getHits().getMaxScore());
		do {
			for(SearchHit hit : response.getHits().getHits()) {
				System.out.println(hit.getSourceAsString());
			}
			
			response = client.prepareSearchScroll(response.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();
		}while(response.getHits().getHits().length != 0);
	}
	
	
	
	
    public static void main( String[] args )
    {
    	/**
    	 * Add transport addresses and do something with the client...
	    	Settings settings = Settings.builder()
	    	        .put("cluster.name", "myClusterName").build();
	    	TransportClient client = new PreBuiltTransportClient(settings);
    	*/
    	Settings settings = Settings.builder()
    			.put("cluster.name", "elasticsearch")
    	        .put("client.transport.sniff", true).build();
    	TransportClient client = new PreBuiltTransportClient(settings);
    	
		try {
			client = client.addTransportAddress(
					new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
			// createIndexResponse(client);
//			createSearchResponse(client);
			//createTermsSearchResponse(client);
			//createMultiSearch(client);
			//createConstantQuery(client);
			createBoolQuery(client);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			client.close();
		}


    }
}
