package com.atguigu.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lc
 * @create 2021-05-31-20:29
 */
public class MySink extends AbstractSink implements Configurable {
        //获取logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);


    //1.定义两个属性(前后缀)
    private String prefix;
    private String subfix;

    public void configure(Context context) {

        //2.读取配置文件，为前后缀赋值
        String prefix = context.getString("prefix");
        String subfix = context.getString("subfix", "atguigu");
    }


    /**
     * 1.获取channel
     * 2.从channel获取事务和数据
     * 3.发送数据
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        //1.定义返回值
        Status status = null;

        //2.获取channel
        Channel channel = getChannel();

        //3.从channel获取事务
        Transaction transaction = channel.getTransaction();

        //4.开启事务
        transaction.begin();

        try {
            //5.从channel获取数据
            Event event = channel.take();

            //6.处理事件
            if (event != null){
                String body = new String(event.getBody());
                logger.info(prefix + body + subfix);//将数据打印到logger里面

            }
            //7.提交事务
            transaction.commit();

            //8.成功提交修改状态信息
            status = Status.READY;

        } catch (ChannelException e) {
            e.printStackTrace();
            //9.提交任务失败
            transaction.rollback();

            //10.修改事务状态
            status = Status.BACKOFF;

        }finally{
            //11.关闭事务
            transaction.close();

        }

            //12.返回状态信息
        return status;
    }

}
