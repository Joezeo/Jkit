package com.toocol.distribute.selector;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 主节点状态监听
 *
 * @author ZhaoZhe (joezane.cn@gmail.com)
 * @date 2022/3/23 11:26
 */
public class MasterLeaderListener implements LeaderSelectorListener {
    private final AtomicBoolean end = new AtomicBoolean();

    private final Runnable then;
    private final String path;

    public MasterLeaderListener(Runnable then, String path) {
        this.then = then;
        this.path = path;
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        System.out.println("当前节点选举成为主节点, path = " + client.getNamespace() + path);
        then.run();
        while (true) {
            if (end.get()) {
                return;
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            end.set(true);
        }
    }
}
