package com.nettyrpc.client;

/**
 * @author luxiaoxun
 * @date 2016/03/17
 */
public interface AsyncRpcCallback {

    /**
     * 异步调用成功
     * @param result
     */
    void success(Object result);

    /**
     * 异步调用异常
     * @param e
     */
    void fail(Exception e);

}
