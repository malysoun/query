/*
 * Copyright (c) 2013, Zenoss and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Zenoss or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.zenoss.app.metricservice.api.impl;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.zenoss.app.annotations.API;
import org.zenoss.app.metricservice.MetricServiceAppConfiguration;
import org.zenoss.app.metricservice.api.configs.MetricServiceConfig;
import org.zenoss.app.metricservice.api.model.MetricSpecification;
import org.zenoss.app.metricservice.api.model.ReturnSet;
import org.zenoss.app.metricservice.api.model.v2.MetricQuery;
import org.zenoss.app.metricservice.api.model.v2.MetricRequest;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@API
@Configuration
@Profile({"default", "prod"})
public class OpenTSDBMetricStorage implements MetricStorageAPI {
    @Autowired
    public MetricServiceAppConfiguration config;

    private static final Logger log = LoggerFactory.getLogger(OpenTSDBMetricStorage.class);

    private static final String SOURCE_ID = "OpenTSDB";

    private static ExecutorService executorServiceInstance = null;

    static final String SPACE_REPLACEMENT = "//-";
    private DefaultHttpClient httpClient = null;


    @Override
    public OpenTSDBQueryReturn query(MetricRequest query) {
        Optional<String> start = Optional.fromNullable(query.getStart());
        Optional<String> end = Optional.fromNullable(query.getEnd());
        //provide defaults
        String startTime = start.or(config.getMetricServiceConfig().getDefaultStartTime());
        String endTime = end.or(config.getMetricServiceConfig().getDefaultEndTime());

        OpenTSDBQuery otsdbQuery = new OpenTSDBQuery();
        // This could maybe be better - for now, it works : end time defaults to 'now', start time does not default.
        otsdbQuery.start = startTime;
        if (!Utils.NOW.equals(endTime)) {
            otsdbQuery.end = endTime;
        }

        for (MetricQuery mq : query.getQueries()) {
            otsdbQuery.addSubQuery(createOTSDBQuery(mq));
        }

        OpenTSDBClient client = new OpenTSDBClient(this.getHttpClient(), getOpenTSDBApiQueryUrl());
        OpenTSDBQueryReturn result = client.query(otsdbQuery);
        for (OpenTSDBQueryResult series : result.getResults()) {
            series.metric = series.metric.replace(SPACE_REPLACEMENT, " ");
        }
        return result;
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.zenoss.app.query.api.impl.MetricStorageAPI#getReader(org.zenoss.app
     * .query.QueryAppConfiguration, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Boolean, java.lang.Boolean, java.util.List)
     */
    @Override
    public List<OpenTSDBQueryResult> getResponse(MetricServiceAppConfiguration config,
                                                 String id, String startTime, String endTime, ReturnSet returnset,
                                                 String downsample, double downsampleMultiplier,
                                                 Map<String, List<String>> globalTags,
                                                 List<MetricSpecification> queries) throws IOException {

        String appliedDownsample = createModifiedDownsampleRequest(downsample, downsampleMultiplier);
        log.debug("Specified Downsample = {}, Specified Multiplier = {}, Applied Downsample = {}.", downsample, downsampleMultiplier, appliedDownsample);

        for (MetricSpecification metricSpecification : queries) {
            String oldDownsample = metricSpecification.getDownsample();
            if (null != oldDownsample && !oldDownsample.isEmpty()) {
                log.info("Overriding specified series downsample ({}) with global specification of {}", oldDownsample, appliedDownsample);
            }
            metricSpecification.setDownsample(appliedDownsample);
        }

        List<OpenTSDBQueryResult> responses = runQueries(startTime, endTime, queries);
        for (OpenTSDBQueryResult result : responses) {
            result.metric = result.metric.replace(SPACE_REPLACEMENT, " ");
        }
        return responses;
    }


    private String getOpenTSDBApiQueryUrl() {
        return String.format("%s/api/query", config.getMetricServiceConfig().getOpenTsdbUrl());
    }

    private static OpenTSDBSubQuery createOTSDBQuery(MetricQuery mq) {
        final boolean allowWildCard = true;
        OpenTSDBSubQuery result = null;
        if (null != mq) {
            result = new OpenTSDBSubQuery();
            result.aggregator = mq.getAggregator();
            result.downsample = mq.getDownsample();

            // escape the name of the metric since OpenTSDB doesn't like spaces
            String metricName = mq.getMetric();
            metricName = metricName.replace(" ", SPACE_REPLACEMENT);
            result.metric = metricName;


            result.rate = mq.getRate();
            result.rateOptions = new OpenTSDBRateOption(mq.getRateOptions());
            Map<String, List<String>> tags = mq.getTags();
            if (null != tags) {
                for (Map.Entry<String, List<String>> tagEntry : tags.entrySet()) {
                    for (String tagValue : tagEntry.getValue()) {
                        //apply metric-consumer sanitization to tags in query
                        result.addTag(Tags.sanitizeKey(tagEntry.getKey()), Tags.sanitizeValue(tagValue, allowWildCard));
                    }
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.zenoss.app.query.api.impl.MetricStorageAPI#getSourceId()
     */
    @Override
    public String getSourceId() {
        return SOURCE_ID;
    }

    private static String parseAggregation(String v) {
        String result = "";
        int dashPosition = v.indexOf('-');
        if (dashPosition > 0 && dashPosition < v.length()) {
            result = v.substring(dashPosition + 1);
        }
        return result;
    }

    private static String createModifiedDownsampleRequest(String downsample, double downsampleMultiplier) {
        if (null == downsample || downsample.isEmpty() || downsampleMultiplier <= 0.0) {
            log.warn("Bad downsample or multiplier. Returning original downsample value of {}.", downsample);
            return downsample;
        }
        long duration = Utils.parseDuration(downsample);
        String aggregation = parseAggregation(downsample);
        long newDuration = (long) (duration / downsampleMultiplier);
        if (newDuration <= 0) {
            log.warn("Applying value {} of downsampleMultiplier to downsample value of {} would result in a request with resolution finer than 1 sec. returning 1 second.", downsampleMultiplier, downsample);
            newDuration = 1;
        }
        return String.format("%ds-%s", newDuration, aggregation);
    }

    private List<OpenTSDBQueryResult> runQueries(String start, String end, List<MetricSpecification> queries) {
        List<Callable<OpenTSDBQueryResult>> callables = new ArrayList<>(queries.size());
        DefaultHttpClient httpClient = getHttpClient();
        for (MetricSpecification mSpec : queries) {
            MetricSpecCallable callable = new MetricSpecCallable(httpClient, start, end, mSpec, getOpenTSDBApiQueryUrl());
            callables.add(callable);
        }
        List<Future<OpenTSDBQueryResult>> futures = invokeCallables(callables);
        log.debug("{} futures returned.", futures.size());
        List<OpenTSDBQueryResult> results = new ArrayList<>();
        getResultsFromFutures(results, futures);
        log.debug("{} results returned.", results.size());
        return results;
    }

    private List<Future<OpenTSDBQueryResult>> invokeCallables(List<Callable<OpenTSDBQueryResult>> callables) {
        ExecutorService executorService = getExecutorService();
        List<Future<OpenTSDBQueryResult>> futures = new ArrayList<>();
        try {
            log.debug("invoking {} callables...", callables.size());
            futures = executorService.invokeAll(callables); // throws: InterruptedException (checked), NullPointerException/RejectedExecutionException (unchecked)
        } catch (InterruptedException | NullPointerException | RejectedExecutionException e) {
            log.error("Query execution was unsuccessful: {}", e.getMessage());
        }
        return futures;
    }

    private ExecutorService getExecutorService() {
        return executorServiceInstance;
    }

    private void getResultsFromFutures(List<OpenTSDBQueryResult> results, List<Future<OpenTSDBQueryResult>> futures) {
        for (Future<OpenTSDBQueryResult> future : futures) {
            try {
                OpenTSDBQueryResult result = future.get(); // Throws InterruptedException, ExecutionException (checked); CancellationException (unchecked)
                results.add(result);
            } catch (InterruptedException | ExecutionException | CancellationException e) {
                // On exception, return an empty result, with the queryStatus set to indicate the problem.
                OpenTSDBQueryResult result = new OpenTSDBQueryResult();
                QueryStatus queryStatus = new QueryStatus(QueryStatus.QueryStatusEnum.ERROR, e.getMessage());
                result.setStatus(queryStatus);
                log.error("{} exception getting result from future: {}", e.getClass().getName(), e.getMessage());
            }
        }
    }

    private DefaultHttpClient getHttpClient() {
        return httpClient;
    }

    @PostConstruct
    public void startup() {
        log.debug("**************** PostConstruct method called. ***********");
        makeHttpClient();
        int executorThreadPoolMaxSize = config.getMetricServiceConfig().getExecutorThreadPoolMaxSize();
        int executorThreadPoolCoreSize = config.getMetricServiceConfig().getExecutorThreadPoolCoreSize();
        if (executorThreadPoolCoreSize > executorThreadPoolMaxSize) {
            log.warn("executorThreadPool max size ({}) is less than core size ({}). Using specified max ({}) for both values.", executorThreadPoolMaxSize, executorThreadPoolCoreSize, executorThreadPoolMaxSize);
            executorThreadPoolCoreSize = executorThreadPoolMaxSize;
        }
        log.info("Setting up executor pool with {}-{} threads.", executorThreadPoolCoreSize, executorThreadPoolMaxSize);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("TSDB-query-thread-%d").build();
        executorServiceInstance = new ThreadPoolExecutor(executorThreadPoolCoreSize, executorThreadPoolMaxSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), namedThreadFactory);
    }

    private void makeHttpClient() {
        log.info("Creating new PoolingClientConnectionManager.");
        MetricServiceConfig conf = config.getMetricServiceConfig();
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        int maxTotalPoolConnections = conf.getMaxTotalPoolConnections();
        int maxPoolConnectionsPerRoute = conf.getMaxPoolConnectionsPerRoute();
        log.debug("Setting up pool with {} total connections and {} max connections per route.", maxTotalPoolConnections, maxPoolConnectionsPerRoute);
        cm.setMaxTotal(maxTotalPoolConnections);
        cm.setDefaultMaxPerRoute(maxPoolConnectionsPerRoute);
        httpClient = new DefaultHttpClient(cm);
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setSoTimeout(httpParams, conf.getHttpSocketTimeoutMs());
        HttpConnectionParams.setConnectionTimeout(httpParams, conf.getConnectionTimeoutMs());
        httpParams.setParameter(ClientPNames.CONN_MANAGER_TIMEOUT, new Long(conf.getConnectionManagerTimeoutMs()));
    }

    @PreDestroy
    public void shutdown() {
        log.debug("************* PreDestroy method called. ****************");
        httpClient.getConnectionManager().shutdown();
        httpClient = null;
    }
}
