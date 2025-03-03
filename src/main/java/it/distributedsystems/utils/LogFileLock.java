package it.distributedsystems.utils;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogFileLock {
    private static final ReentrantReadWriteLock logUpdateLock = new ReentrantReadWriteLock(true);


}
