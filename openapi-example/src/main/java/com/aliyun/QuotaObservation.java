package com.aliyun;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.maxcompute20220104.Client;
import com.aliyun.maxcompute20220104.models.GetQuotaUsageRequest;
import com.aliyun.maxcompute20220104.models.GetQuotaUsageResponse;
import com.aliyun.maxcompute20220104.models.GetQuotaUsageResponseBody.GetQuotaUsageResponseBodyData;
import com.aliyun.teaopenapi.models.Config;

/**
 * @author jiangyefan
 */
public class QuotaObservation {

    private Client client;

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

    // aliyun API ref: https://api.aliyun.com/document/MaxCompute/2022-01-04/GetQuotaUsage
    public String getQuotaUsage(long startTimestamp, long endTimestamp, String quotaNickname,
        String subQuotaNickname, double resourceUsageThreshold, double resourceRequestThreshold,
        double countThreshold) {

        init();

        /**
         * Build GetQuotaUsage API for current time quota's usage.
         * ATTENTION: the collection frequency of Quota usage metric is 1 minute
         */
        GetQuotaUsageRequest getQuotaUsageRequest = new GetQuotaUsageRequest();
        // The time unit is seconds.
        // Suggest: Specify a time range within the last half hour.
        getQuotaUsageRequest.setFrom(startTimestamp);
        getQuotaUsageRequest.setTo(endTimestamp);
        getQuotaUsageRequest.setSubQuotaNickname(subQuotaNickname);
        getQuotaUsageRequest.setProductId("ODPS");
        getQuotaUsageRequest.setAggMethod("max");

        try {
            // Get Quota usage metric
            GetQuotaUsageResponse response = client.getQuotaUsage(quotaNickname, getQuotaUsageRequest);
            GetQuotaUsageResponseBodyData quotaUsageData = response.getBody().getData();

            Map<String, Object> metricMap = (Map<String, Object>)quotaUsageData.getMetrics();

            // Get the metric data according to the metric name
            /**
             * timestamps
             * minCpu
             * maxCpu
             * usedCpu (only in subscription quota)
             * postpay-usedCpu (only in payasyougo quota)
             * adhocCpu
             * adhocUsedCpu
             * requestCpu
             * totalMaxCpu(Numerically, totalMaxCpu = maxCpu + adhocCpu)
             * totalUsedCpu(Numerically, totalUsedCpu = usedCpu + adhocUsedCpu)
             */
            List<Long> timestampList = (List<Long>)metricMap.get("timestamps");
            List<Double> adhocCpuList = (List<Double>)metricMap.get("adhocCpu");
            List<Double> minCpuList = (List<Double>)metricMap.get("minCpu");
            List<Double> totalUsedCpuList = (List<Double>)metricMap.get("totalUsedCpu");
            List<Double> requestCpuList = (List<Double>)metricMap.get("requestCpu");
            List<Double> cpuUsedList = (List<Double>)metricMap.get("usedCpu");

            int count = 0;
            List<Long> accumulatedTimestampList = new ArrayList<>();
            for (int i = 0; i < timestampList.size(); i++) {
                Long timestamp = timestampList.get(i);

                // totalCpu not always equal to totalMaxCpu. Suggest use totalCpu.
                Double totalCpu = minCpuList.get(i) + adhocCpuList.get(i);
                Double totalUsedCpu = totalUsedCpuList.get(i);

                double resourceUsageRate = totalCpu == 0 ? 0 : totalUsedCpu / totalCpu;

                // if resource usage rate is over resourceUsageThreshold, determine whether resource requests are
                // accumulating.
                // 95% is just a reference value for resourceUsageThreshold
                if (resourceUsageRate >= resourceUsageThreshold) {
                    Double requestCpu = requestCpuList.get(i);
                    Double cpuUsed = cpuUsedList.get(i);
                    double resourceRequestRate = cpuUsed == 0 ? 0 : requestCpu / cpuUsed;

                    // if resource requests are accumulating, count it.
                    // 10 is just a reference value for resourceRequestThreshold
                    if (resourceRequestRate >= resourceRequestThreshold) {
                        count++;
                        accumulatedTimestampList.add(timestamp);

                    }

                }

            }

            // if the count is over countThreshold for the timestampList, it means that resource requests are
            // accumulating.
            // 0.5 is just a reference value for countThreshold
            if ((double)count / timestampList.size() >= countThreshold) {
                System.out.println("Quota usage is over threshold");
                System.out.println("Accumulated timestamp list: " + accumulatedTimestampList);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }
}
