package com.nettyrpc.client.proxy;

import com.nettyrpc.client.ConnectManage;
import com.nettyrpc.client.RpcFuture;
import com.nettyrpc.client.RpcClientHandler;
import com.nettyrpc.protocol.RpcRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

/**
 * @author luxiaoxun
 * @date 2016/03/16
 */
public class ObjectProxy<T> implements InvocationHandler, IAsyncObjectProxy {

    private final String EQUALS_METHOD_NAME = "equals";
    private final String HASHCODE_METHOD_NAME = "hashCode";
    private final String TOSTRING_METHOD_NAME = "toString";

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectProxy.class);
    private Class<T> clazz;

    public ObjectProxy(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Object.class == method.getDeclaringClass()) {
            String name = method.getName();
            if (EQUALS_METHOD_NAME.equals(name)) {
                return proxy == args[0];
            } else if (HASHCODE_METHOD_NAME.equals(name)) {
                return System.identityHashCode(proxy);
            } else if (TOSTRING_METHOD_NAME.equals(name)) {
                return proxy.getClass().getName() + "@" +
                        Integer.toHexString(System.identityHashCode(proxy)) +
                        ", with InvocationHandler " + this;
            } else {
                throw new IllegalStateException(String.valueOf(method));
            }
        }

        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setClassName(method.getDeclaringClass().getName());
        request.setMethodName(method.getName());
        request.setParameterTypes(method.getParameterTypes());
        request.setParameters(args);
        // Debug
        LOGGER.debug(method.getDeclaringClass().getName());
        LOGGER.debug(method.getName());
        for (int i = 0; i < method.getParameterTypes().length; ++i) {
            LOGGER.debug(method.getParameterTypes()[i].getName());
        }
        for (Object arg : args) {
            LOGGER.debug(arg.toString());
        }

        RpcClientHandler handler = ConnectManage.getInstance().chooseHandler();
        RpcFuture rpcFuture = handler.sendRequest(request);
        return rpcFuture.get();
    }

    @Override
    public RpcFuture call(String funcName, Object... args) {
        RpcClientHandler handler = ConnectManage.getInstance().chooseHandler();
        RpcRequest request = createRequest(this.clazz.getName(), funcName, args);
        return handler.sendRequest(request);
    }

    private RpcRequest createRequest(String className, String methodName, Object[] args) {
        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setClassName(className);
        request.setMethodName(methodName);
        request.setParameters(args);

        Class[] parameterTypes = new Class[args.length];
        // Get the right class type
        for (int i = 0; i < args.length; i++) {
            parameterTypes[i] = getClassType(args[i]);
        }

        request.setParameterTypes(parameterTypes);

        LOGGER.debug(className);
        LOGGER.debug(methodName);
        for (Class parameterType : parameterTypes) {
            LOGGER.debug(parameterType.getName());
        }
        for (Object arg : args) {
            LOGGER.debug(arg.toString());
        }

        return request;
    }

    private Class<?> getClassType(Object obj) {
        Class<?> classType = obj.getClass();
        String typeName = classType.getName();
        switch (typeName) {
            case "java.lang.Integer":
                return Integer.TYPE;
            case "java.lang.Long":
                return Long.TYPE;
            case "java.lang.Float":
                return Float.TYPE;
            case "java.lang.Double":
                return Double.TYPE;
            case "java.lang.Character":
                return Character.TYPE;
            case "java.lang.Boolean":
                return Boolean.TYPE;
            case "java.lang.Short":
                return Short.TYPE;
            case "java.lang.Byte":
                return Byte.TYPE;
            default:
                return classType;
        }
    }
}