package com.atguigu.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lc
 * @create 2021-05-30-20:32
 */
public class TypeInterceptor implements Interceptor {

    //声明一个存放事件的集合
    private List<Event> addHeaderEvents;


    public void initialize() {
        //初始化
        addHeaderEvents = new ArrayList<Event>();
    }

    //单个事件拦截
    public Event intercept(Event event) {
        //1.获取事件中的头信息(key-value形式)
        Map<String, String> headers = event.getHeaders();

        //2.获取事件中的body信息
        String body = new String(event.getBody());

        //3.根据body中是否含有“hello”来决定添加怎样的头信息

            //4.添加头信息
        if (body.contains("hello")){
            headers.put("topic" , "first");
        }else {
            headers.put("topic" , "second");
        }
        return event;
    }

    //批量事件拦截
    public List<Event> intercept(List<Event> list) {

        //1.清空集合
        addHeaderEvents.clear();

        //2.遍历events
        for (Event event : list) {
            //3.给每一个事件添加头信息
            addHeaderEvents.add(intercept(event));
        }
        //4.返回结果
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
