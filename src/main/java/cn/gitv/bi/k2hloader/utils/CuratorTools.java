package cn.gitv.bi.k2hloader.utils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by Kang on 2017/7/23.
 */
public class CuratorTools {
    public CuratorFramework zkClient = null;
    private static CuratorTools curatorTools;

    private CuratorTools(String zkConnectString, int sessionTimeout, int connectTimeout) {
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);// 重试机制
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkConnectString).connectionTimeoutMs(connectTimeout)
                .sessionTimeoutMs(sessionTimeout).retryPolicy(rp);
        CuratorFramework zkClient = builder.build();
        this.zkClient = zkClient;
        this.zkClient.start();// 放在这前面执行
    }

    public static CuratorTools getInstance(String zkConnectString, int sessionTimeout, int connectTimeout) {
        if (curatorTools == null) {
            synchronized (CuratorTools.class) {
                if (curatorTools == null) {
                    curatorTools = new CuratorTools(zkConnectString, sessionTimeout, connectTimeout);
                }
            }
        }
        return curatorTools;
    }


}
