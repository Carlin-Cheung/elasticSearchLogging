package org.elasticsearch.client.transport;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.tz.Provider;
import org.joda.time.tz.UTCProvider;

public class LoggingSearch {
	
	
	/**
	 * term query
	 * @param client
	 */
public static void createTermsSearchResponse(TransportClient client)  {
		
		QueryBuilder query = termsQuery("log",    
		    "error", "Exception", "Warning");   
		
		DateTime endDate = new DateTime(System.currentTimeMillis() + 8 * 60 * 60 * 1000);
		DateTime startDate = new DateTime(endDate.getMillis() - 30 * 60 * 1000);
		
		System.out.println(System.currentTimeMillis());
		System.out.println(startDate.getMillis());
		System.out.println(endDate.getMillis());
		
		SearchResponse response = client.prepareSearch("logstash-*")
		        .setTypes("fluentd")
		        .addSort("@timestamp", SortOrder.DESC)
		        .setScroll(new TimeValue(60000))
		        .setQuery(query)                 // Query
		        .setPostFilter(QueryBuilders.rangeQuery("@timestamp").from(startDate).to(endDate))     // Filter: accord to the time sort
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
			    //.must(termQuery("kubernetes.host", "lab3"))
			    .must(termQuery("kubernetes.labels.name", "iotdb-master"))
			    ;
		
		SearchResponse response = client.prepareSearch("logstash-*")
		        .setTypes("fluentd")
		        .addSort("@timestamp", SortOrder.DESC)
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)                 // Query
		        .setSize(100).setExplain(true)
		        .get();
		
		do {
			for(SearchHit hit : response.getHits().getHits()) {
				System.out.println(hit.getSourceAsString());
			}
			
			response = client.prepareSearchScroll(response.getScrollId())
						.setScroll(new TimeValue(60000))
						.execute()
						.actionGet();
		}while(response.getHits().getHits().length != 0);
	}
	
	public static void main(String[] args) {
		/**
		 * Add transport addresses and do something with the client... Settings
		 * settings = Settings.builder() .put("cluster.name",
		 * "myClusterName").build(); TransportClient client = new
		 * PreBuiltTransportClient(settings);
		 */
		Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch")
				.put("client.transport.sniff", true)
				.build();
		TransportClient client = new PreBuiltTransportClient(settings);

		try {
			client.addTransportAddress(
					new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
			createTermsSearchResponse(client);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
	}

}
