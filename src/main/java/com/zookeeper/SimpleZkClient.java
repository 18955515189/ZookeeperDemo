package com.zookeeper;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * 简单的zookeeper实现程序
 * Created by david on 2018/5/27.
 */
public class SimpleZkClient {

    private static final String connectString = "hadoop01:2081,hadoop02:2181,hadoop03:2181";
    private static final int sessionTimeOut = 2000;
    CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zkClient = null ;

    /**
     * 创建ZK 客户端程序
     * @throws Exception
     */
    @Before
    public void init() throws Exception{
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if( latch.getCount()>0 && watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    System.out.println( "countdown" );
                    latch.countDown();
                }

                //收到事件通知后的回调函数（ 业务处理逻辑 ）
                System.out.println( watchedEvent.getType() +"-  -  -  -  -  - "+ watchedEvent.getPath() );
                System.out.println( watchedEvent.getState() );

                /*try {
                    zkClient.getData( "/test1",true,null );
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/

            }
        });
        latch.await();
    }

    /**
     * 测试创建节点
     */
    @Test
    public void testCreate(){
        try {
            String node = zkClient.create( "/mygirls","helloworld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println( "创建成功 :"+ node );
            zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
