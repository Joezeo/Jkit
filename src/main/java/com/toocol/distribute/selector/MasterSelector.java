package com.toocol.distribute.selector;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;

/**
 * 基于Curator/Zookeeper进行主节点选举
 *
 * @author ZhaoZhe (joezane.cn@gmail.com)
 * @date 2022/3/23 11:23
 */
public class MasterSelector {
    private static final String BASE_PATH = "master";
    private final LeaderSelector leaderSelector;

    public MasterSelector(CuratorFramework curatorFramework, String serverName, Runnable then) {
        String path = "/" + BASE_PATH + "/" + serverName;
        this.leaderSelector = new LeaderSelector(curatorFramework, path, new MasterLeaderListener(then, path));
        this.leaderSelector.autoRequeue();
    }

    public void select() {
        this.leaderSelector.start();
    }
}
