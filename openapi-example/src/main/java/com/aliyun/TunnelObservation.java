package com.aliyun;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.maxcompute20220104.Client;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricDetailRequest;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricDetailResponse;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricDetailResponseBody.QueryTunnelMetricDetailResponseBodyData;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricDetailResponseBody.QueryTunnelMetricDetailResponseBodyDataMetrics;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricRequest;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricResponse;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricResponseBody.QueryTunnelMetricResponseBodyData;
import com.aliyun.maxcompute20220104.models.QueryTunnelMetricResponseBody.QueryTunnelMetricResponseBodyDataMetrics;
import com.aliyun.teaopenapi.models.Config;
import com.google.common.collect.Lists;

/**
 * @author jiangyefan
 */
public class TunnelObservation {

    private Client client;

    public TunnelObservation() {
        init();
    }

    private void init() {

        Properties odpsConfig = new Properties();
        InputStream is =
            Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        try {
            odpsConfig.load(is);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String accessId = odpsConfig.getProperty("access_id");
        String accessKey = odpsConfig.getProperty("access_key");
        String endpoint = odpsConfig.getProperty("endpoint");

        Config config = new Config();

        config.setAccessKeyId(accessId);
        config.setAccessKeySecret(accessKey);
        config.setEndpoint(endpoint);

        try {
            client = new Client(config);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    /**
     * @param metric         metric name, including:
     *                       slot_usage
     *                       slot_max
     *                       throughput
     *                       throughput_speed
     *                       request
     * @param startTimestamp unit: second
     * @param endTimestamp   unit: second
     * @param quotaNickname  quota nickname. `default` or `quotaNickname#subQuotaNickname`
     * @param project        project name.
     *                       project and quotaNickname cannot empty at same time
     * @param tableList      table name,
     *                       if not empty, `project` is necessary
     * @param groupList      necessary. metric display dimensions. including:
     *                       quota
     *                       project
     *                       table
     *                       operation
     *                       ip(only when metric is `throughput`)
     *                       code(only when metric is `request`)
     * @param operationList  operation, including:
     *                       upload - Tunnel Batch Upload
     *                       download - Tunnel Batch Download
     *                       stream_upload - Tunnel Stream Upload
     *                       max_storage_read - Storage API Download
     *                       download_instance - Tunnel Instance Download
     *                       upsert - Tunnel Upsert Upload
     *                       max_storage_write - Storage API Upload
     *                       table_preview - Preview Download
     * @param codeList       http status code
     * @return metric data
     */
    private QueryTunnelMetricResponseBodyData getTunnelMetric(String metric, long startTimestamp, long endTimestamp,
        String quotaNickname, String project, List<String> tableList, List<String> groupList,
        List<String> operationList, List<Integer> codeList, Integer topN) {

        QueryTunnelMetricRequest queryTunnelMetricRequest = new QueryTunnelMetricRequest();
        queryTunnelMetricRequest.setStartTime(startTimestamp);
        queryTunnelMetricRequest.setEndTime(endTimestamp);
        queryTunnelMetricRequest.setStrategy("max");
        queryTunnelMetricRequest.setQuotaNickname(quotaNickname);
        queryTunnelMetricRequest.setProject(project);
        queryTunnelMetricRequest.setTableList(tableList);
        queryTunnelMetricRequest.setGroupList(groupList);
        queryTunnelMetricRequest.setOperationList(operationList);
        queryTunnelMetricRequest.setCodeList(codeList);
        queryTunnelMetricRequest.setTopN(topN);

        try {
            QueryTunnelMetricResponse response = client.queryTunnelMetric(metric, queryTunnelMetricRequest);
            return response.getBody().getData();
        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new RuntimeException("getTunnelMetric failed.");
    }

    /**
     * slot usage by quota or project level.
     * 1. quota and project cannot empty at same time
     * 2. when quota is `default`, groupBy project is necessary
     * 3. when quota is not `default`, groupBy quota is necessary
     */
    public void getSlotUsageByQuota() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        String project = null;
        List<String> tableList = null;
        // Shared quota(default) is a project-level resource, so groupBy project is necessary
        List<String> groupList = Lists.newArrayList("quota", "project");
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("slot_usage", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * slot usage by table level.
     * 1. quota and project is necessary
     * 2. groupBy table is necessary
     * 3. use topN to get topN table's slot usage. topN > 0 and TopN <= 100
     */
    public void getSlotUsageByTable() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        // tableList is not empty, so project is necessary and tableList belongs to project
        String project = "projectName";
        // optional.
        List<String> tableList = Lists.newArrayList("tab1", "tab2");
        // if you want to get table's level metric, groupBy table is necessary.
        List<String> groupList = Lists.newArrayList("quota", "project", "table");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        Integer topN = 20;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("slot_usage", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * slot max only support quota level
     * 1. quota is necessary
     * 2. when quota is `default`, project is necessary and groupBy project is necessary
     * 3. when quota is not `default`, project is null and  groupBy quota is necessary
     */
    public void getSlotMax() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        // will ignore
        String project = null;
        // not support
        List<String> tableList = null;
        List<String> groupList = Lists.newArrayList("quota");
        // not support
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("slot_max", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * slot max only support quota level
     * 1. quota is necessary
     * 2. when quota is `default`, project is necessary and groupBy project is necessary
     * 3. when quota is not `default`, project is null and  groupBy quota is necessary
     */
    public void getSlotMax2() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        // necessary
        String project = "projectName";
        // will ignore
        List<String> tableList = null;
        List<String> groupList = Lists.newArrayList("quota", "project");
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("slot_max", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * throughput by quota or project level.
     * 1. quota and project cannot empty at same time
     */
    public void getThroughputByQuota() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        String project = "projectName";
        List<String> tableList = null;
        List<String> groupList = Lists.newArrayList("quota", "project");
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("throughput", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * throughput by table level.
     * 1. quota and project is necessary
     * 2. groupBy table is necessary
     */
    public void getThroughputByTable() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        // tableList is not empty, so project is necessary and tableList belongs to project
        String project = "projectName";
        // optional
        List<String> tableList = Lists.newArrayList("tab1", "tab2");
        // if you want to get table's level metric, groupBy table is necessary.
        List<String> groupList = Lists.newArrayList("quota", "project", "table");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        Integer topN = 20;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("throughput", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * throughput by ip level.
     * 1. quota and project is necessary
     * 2. tableList is necessary
     * 3. groupBy ip is necessary
     */
    public void getThroughputByIp() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        // tableList is not empty, so project is necessary and tableList belongs to project
        String project = "projectName";
        // tableList is necessary
        List<String> tableList = Lists.newArrayList("tab1", "tab2");
        // if you want to get ip's level metric, groupBy table and ip is necessary.
        List<String> groupList = Lists.newArrayList("quota", "project", "table", "ip");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        // not support
        List<Integer> codeList = null;
        Integer topN = 20;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("throughput", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * throughput speed
     * 1. quota and project cannot empty at same time
     */
    public void getThroughputSpeed() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        String project = null;
        List<String> tableList = null;
        List<String> groupList = Lists.newArrayList("quota", "operation");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = Lists.newArrayList("upload", "download");
        // not support
        List<Integer> codeList = null;
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("throughput_speed", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * request
     * 1. quota and project cannot empty at same time
     */
    public void getRequest() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        ;
        String project = "projectName";
        List<String> tableList = null;
        List<String> groupList = Lists.newArrayList("quota", "project", "code");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        // optional. If you want to get all http code, you can ignore it.
        // if you want to get specific http code, you can set it, and set `code` to groupList too.
        List<Integer> codeList = Lists.newArrayList(400, 403, 500);
        // not support
        Integer topN = null;

        QueryTunnelMetricResponseBodyData data = getTunnelMetric("request", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, codeList, topN);

        for (QueryTunnelMetricResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            List<List<Double>> metricValues = metric.getValues();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            for (List<Double> metricValue : metricValues) {
                System.out.println("timestamp: " + metricValue.get(0) + ", value: " + metricValue.get(1));
            }
        }
    }

    /**
     * @param metric         metric name, including:
     *                       slot_usage_detail
     *                       throughput_detail
     *                       throughput_summary
     * @param startTimestamp unit: second
     * @param endTimestamp   unit: second
     * @param quotaNickname  quota nickname. `default` or `quotaNickname#subQuotaNickname`
     * @param project        project name.
     *                       project and quotaNickname cannot empty at same time
     * @param tableList      table name,
     *                       if not empty, `project` is necessary
     * @param groupList      necessary. metric display dimensions. including:
     *                       quota
     *                       project
     *                       table
     *                       operation
     *                       ip(only when metric is `throughput_detail`)
     * @param operationList  operation, including:
     *                       upload - Tunnel Batch Upload
     *                       download - Tunnel Batch Download
     *                       stream_upload - Tunnel Stream Upload
     *                       max_storage_read - Storage API Download
     *                       download_instance - Tunnel Instance Download
     *                       upsert - Tunnel Upsert Upload
     *                       max_storage_write - Storage API Upload
     *                       table_preview - Preview Download
     * @param orderColumn    order column. including:
     *                       maxValue
     *                       minValue
     *                       avgValue
     *                       sumValue(only when metric is `throughput_summary`)
     * @param isAsc          is ascending order
     * @param limit          data limit
     * @return metric detail data
     */
    private QueryTunnelMetricDetailResponseBodyData getTunnelMetricDetail(String metric, long startTimestamp,
        long endTimestamp,
        String quotaNickname, String project, List<String> tableList, List<String> groupList,
        List<String> operationList, String orderColumn, Boolean isAsc, Long limit) {

        QueryTunnelMetricDetailRequest queryTunnelMetricRequest = new QueryTunnelMetricDetailRequest();
        queryTunnelMetricRequest.setStartTime(startTimestamp);
        queryTunnelMetricRequest.setEndTime(endTimestamp);
        queryTunnelMetricRequest.setQuotaNickname(quotaNickname);
        queryTunnelMetricRequest.setProject(project);
        queryTunnelMetricRequest.setTableList(tableList);
        queryTunnelMetricRequest.setGroupList(groupList);
        queryTunnelMetricRequest.setOperationList(operationList);
        queryTunnelMetricRequest.setOrderColumn(orderColumn);
        queryTunnelMetricRequest.setAscOrder(isAsc);
        queryTunnelMetricRequest.setLimit(limit);

        try {
            QueryTunnelMetricDetailResponse response = client.queryTunnelMetricDetail(metric, queryTunnelMetricRequest);
            return response.getBody().getData();
        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new RuntimeException("getTunnelMetricDetail failed.");
    }

    /**
     * slot_usage_detail
     */
    public void getSlotUsageDetailByTable() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        // tableList is not empty, so project is necessary and tableList belongs to project
        String project = "projectName";
        // optional.
        List<String> tableList = Lists.newArrayList("tab1", "tab2");
        // if you want to get table's level metric, groupBy table is necessary.
        List<String> groupList = Lists.newArrayList("quota", "project", "table");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        String orderColumn = "maxValue";
        Boolean isAsc = false;
        Long limit = 20L;

        QueryTunnelMetricDetailResponseBodyData data = getTunnelMetricDetail("slot_usage_detail", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, orderColumn, isAsc, limit);

        for (QueryTunnelMetricDetailResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            Map<String, Double> metricValues = (Map<String, Double>)metric.getValue();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            metricValues.forEach((k, v) -> System.out.println(k + " : " + v));
        }
    }

    public void getThroughputDetailByIp() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "quotaNickname#subQuotaNickname";
        // tableList is not empty, so project is necessary and tableList belongs to project
        String project = "projectName";
        // tableList is necessary
        List<String> tableList = Lists.newArrayList("tab1", "tab2");
        // if you want to get ip's level metric, groupBy table and ip is necessary.
        List<String> groupList = Lists.newArrayList("quota", "project", "table", "ip");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        String orderColumn = "minValue";
        Boolean isAsc = false;
        Long limit = 20L;

        QueryTunnelMetricDetailResponseBodyData data = getTunnelMetricDetail("throughput_detail", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, orderColumn, isAsc, limit);

        for (QueryTunnelMetricDetailResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            Map<String, Double> metricValues = (Map<String, Double>)metric.getValue();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            metricValues.forEach((k, v) -> System.out.println(k + " : " + v));
        }
    }

    public void getThroughputSummaryDetailByQuota() {
        long startTimeStamp = System.currentTimeMillis() / 1000 - 3600;
        long endTimeStamp = System.currentTimeMillis() / 1000;
        String quotaNickname = "default";
        String project = null;
        List<String> tableList = null;
        // if you want to get operation's level metric, groupBy operation is necessary.
        List<String> groupList = Lists.newArrayList("quota", "operation");
        // optional. If you want to get all operation, you can ignore it.
        // if you want to get specific operation, you can set it, and set `operation` to groupList too.
        List<String> operationList = null;
        String orderColumn = "sumValue";
        Boolean isAsc = false;
        Long limit = 10L;

        QueryTunnelMetricDetailResponseBodyData data = getTunnelMetricDetail("throughput_summary", startTimeStamp, endTimeStamp,
            quotaNickname, project, tableList, groupList, operationList, orderColumn, isAsc, limit);

        for (QueryTunnelMetricDetailResponseBodyDataMetrics metric : data.getMetrics()) {
            Map<String, String> metricBasicInfo = metric.getMetric();
            Map<String, Double> metricValues = (Map<String, Double>)metric.getValue();

            System.out.println("metricBasicInfo: " + metricBasicInfo);
            metricValues.forEach((k, v) -> System.out.println(k + " : " + v));
        }
    }

}
