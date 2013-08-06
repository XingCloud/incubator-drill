package com.xingcloud.meta;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/5/13
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class MetaStoreException extends Exception{

    public MetaStoreException() {
    }

    public MetaStoreException(String message) {
        super(message);
    }

    public MetaStoreException(Throwable cause) {
        super(cause);
    }

    public MetaStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetaStoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
