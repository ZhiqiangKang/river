package com.xiwei.river.exception;

public class RiverException extends RuntimeException{

    public RiverException() {}

    public RiverException(String message) {
        super(message);
    }

    public RiverException(String message, Throwable cause) {
        super(message, cause);
    }

    public RiverException(Throwable cause) {
        super(cause);
    }

    public RiverException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
