package com.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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

                try {
                    zkClient.getData( "/test1",true,null );
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

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
            String node = zkClient.create( "/mygirls","helloworld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println( "创建成功 :"+ node );
            zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试获取节点数据 c测试watcher 工作
     */
    @Test
    public void testGet(){
        try {
            String value = String.valueOf(zkClient.getData( "/test1",true,null ));
            System.out.println( "获取test1数据成功 :"+ value );
            Thread.sleep( Integer.MAX_VALUE );
           // zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试删除节点
     */
    @Test
    public void testDelete(){
        try {
             zkClient.delete( "/mygirl",-1 );
            System.out.println(" 删除成功 ");
             zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试修改节点数据
     */
    @Test
    public void testUpdate(){
        try {
            zkClient.setData( "/mygirls","aaaaa".getBytes(),-1 );
            System.out.println(" 修改成功 ");
            zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试判断节点是否存在
     */
    @Test
    public void testExists(){
        try {
            Stat stat = zkClient.exists( "/mygirl",true );
            System.out.println( stat==null ? "不存在":"存在" );
            zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
