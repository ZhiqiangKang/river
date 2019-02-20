package com.xiwei.river.util;

import com.xiwei.river.exception.RiverException;

import java.lang.reflect.Constructor;

public class ExceptionUtil {

    public static void throwException(Class<? extends RuntimeException> runtimeExceptionClass, String message) {

        RuntimeException runtimeException = null;
        try {
            Constructor<? extends RuntimeException> constructor = runtimeExceptionClass.getConstructor(String.class);
                    runtimeException = constructor.newInstance(message);
        } catch (Exception e) {
            throw new RiverException(e);
        }

        throw runtimeException;
    }
}
