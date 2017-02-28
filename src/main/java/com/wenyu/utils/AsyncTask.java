package com.wenyu.utils;

import java.util.concurrent.Callable;

/**
 * Created by wenyu on 2/28/17.
 */
public abstract class AsyncTask<T> implements Callable<T> {
    public boolean preExecute() {
        return true;
    }

    public abstract T execute();

    public boolean postExecute() {
        return true;
    }

    public T call() throws Exception {
        try {
            preExecute();
            T result = execute();
            postExecute();
            return result;
        } catch (Throwable ex) {
            throw new Exception(ex.toString());
        }
    }
}
