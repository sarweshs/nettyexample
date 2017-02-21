package com.yuktix.netty;

public interface OutputConnection  {

    void outputMetric(String metric) throws Exception ;

    void connect() throws Exception;

    void destroy() throws Exception;
}
