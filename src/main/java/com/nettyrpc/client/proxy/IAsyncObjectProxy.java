package com.nettyrpc.client.proxy;

import com.nettyrpc.client.RpcFuture;

/**
 * @author luxiaoxun
 * @date 2016/3/16
 */
public interface IAsyncObjectProxy {
    /**
     * 异步调用接口
     * @param funcName 方法名
     * @param args 方法参数
     * @return
     */
    public RpcFuture call(String funcName, Object... args);
}