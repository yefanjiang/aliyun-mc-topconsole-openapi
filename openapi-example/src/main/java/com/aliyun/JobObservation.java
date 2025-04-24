package com.aliyun;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.aliyun.maxcompute20220104.Client;
import com.aliyun.maxcompute20220104.models.GetJobInfoResponse;
import com.aliyun.maxcompute20220104.models.GetJobInfoResponseBody.GetJobInfoResponseBodyData;
import com.aliyun.maxcompute20220104.models.GetJobInfoResponseBody.GetJobInfoResponseBodyDataJobSubStatusList;
import com.aliyun.maxcompute20220104.models.ListJobInfosRequest;
import com.aliyun.maxcompute20220104.models.ListJobInfosResponse;
import com.aliyun.maxcompute20220104.models.ListJobInfosResponseBody.ListJobInfosResponseBodyDataJobInfoList;
import com.aliyun.teaopenapi.models.Config;

/**
 * @author jiangyefan
 */
public class JobObservation {

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

    // aliyun API
    // ref: https://api.aliyun.com/document/MaxCompute/2022-01-04/ListJobInfos
    // ref: https://api.aliyun.com/document/MaxCompute/2022-01-04/GetJobInfo
    public void getWaitingJobs(long startTimestamp, long endTimestamp) throws Exception {
        init();

        /**
         * Build ListJobInfo request for all waiting jobs.
         * ATTENTION: this API has 2 or 3 minutes delay
         */
        ListJobInfosRequest listJobInfosRequest = new ListJobInfosRequest();
        // The time unit is seconds.
        // Suggest: Specify a time range within the last half hour. All jobs whose lifecycles overlap with this time range will be returned.
        listJobInfosRequest.setFrom(startTimestamp);
        listJobInfosRequest.setTo(endTimestamp);
        List<String> statusList = new ArrayList<>();
        statusList.add("submitted");
        listJobInfosRequest.setStatusList(statusList);
        listJobInfosRequest.setPageSize(100L);

        // get all waiting jobs.
        ListJobInfosResponse listJobInfosResponse = client.listJobInfos(listJobInfosRequest);
        List<ListJobInfosResponseBodyDataJobInfoList> jobInfoList = listJobInfosResponse.getBody().getData()
            .getJobInfoList();

        // for each job, get it's subStatus info
        for (ListJobInfosResponseBodyDataJobInfoList listJobInfosResponseBodyDataJobInfo : jobInfoList) {
            String instId = listJobInfosResponseBodyDataJobInfo.getInstanceId();

            // get job detail info with subStatusList
            GetJobInfoResponse getJobInfoResponse = client.getJobInfo(instId);
            GetJobInfoResponseBodyData jobInfo = getJobInfoResponse.getBody().getData();
            List<GetJobInfoResponseBodyDataJobSubStatusList> jobSubStatusList = jobInfo.getJobSubStatusList();

            // find the last subStatus order by StartTime.
            GetJobInfoResponseBodyDataJobSubStatusList lastJobSubStatus = jobSubStatusList.get(
                jobSubStatusList.size() - 1);

            // subStatus code and description meaning: https://esg61gffnzc5.sg.larksuite.com/docx/XGZ6dAouto6VQfxJ5pQleabOgDg

            /**
             * 1010	Waiting for scheduling
             * 1020	Waiting for execution
             * 1030	Preparing for execution
             * 1032	Task is executing
             * 1040	Job has been submitted
             * 1051	Online Job Waiting for running
             * 1210	SQLTask is initializing
             * 1220	SQLTask is compiling query
             * 1230	SQLTask is optimizing query
             * 1235	SQLTask is splitting data sources
             * 1240	SQLTask is generating execution plan
             * 1250	SQLTask is submitting execution plan
             * 1052	Online/Offline Job is running
             */

            if (lastJobSubStatus.getCode() == 1010 || lastJobSubStatus.getCode() == 1020
                || lastJobSubStatus.getCode() == 1030 || lastJobSubStatus.getCode() == 1032
                || lastJobSubStatus.getCode() == 1040) {
                System.out.println(
                    String.format("instId:%s is waiting for FE Scheduling. Code: %s, Description: %s, StartTime: %s",
                        instId,
                        lastJobSubStatus.getCode(),
                        lastJobSubStatus.getDescription(),
                        lastJobSubStatus.getStartTime()
                    ));
            }

            if (lastJobSubStatus.getCode() == 1210 || lastJobSubStatus.getCode() == 1220
                || lastJobSubStatus.getCode() == 1230 || lastJobSubStatus.getCode() == 1235
                || lastJobSubStatus.getCode() == 1240) {
                System.out.println(
                    String.format("instId:%s is waiting for SQL processing. Code: %s, Description: %s, StartTime: %s",
                        instId,
                        lastJobSubStatus.getCode(),
                        lastJobSubStatus.getDescription(),
                        lastJobSubStatus.getStartTime()
                    ));
            }

            if (lastJobSubStatus.getCode() == 1250 || lastJobSubStatus.getCode() == 1051) {
                System.out.println(
                    String.format(
                        "instId:%s is waiting for FUXI computing resource. Code: %s, Description: %s, StartTime: %s",
                        instId,
                        lastJobSubStatus.getCode(),
                        lastJobSubStatus.getDescription(),
                        lastJobSubStatus.getStartTime()
                    ));
            }

            // ListJobInfo has 2 or 3 minutes delay. So maybe it is running when you get it. So we need to check it.
            if (lastJobSubStatus.getCode() == 1052) {
                System.out.println(
                    String.format("instId:%s is running. Code: %s, Description: %s, StartTime: %s",
                        instId,
                        lastJobSubStatus.getCode(),
                        lastJobSubStatus.getDescription(),
                        lastJobSubStatus.getStartTime()
                    ));
            }
        }

    }
}
