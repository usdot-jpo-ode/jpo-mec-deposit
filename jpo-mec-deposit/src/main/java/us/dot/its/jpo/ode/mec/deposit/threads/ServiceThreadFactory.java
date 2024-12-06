package us.dot.its.jpo.ode.mec.deposit.threads;

import java.util.concurrent.ThreadFactory;

public class ServiceThreadFactory implements ThreadFactory {

    private String threadName;

    public ServiceThreadFactory(String name) {
        this.threadName = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(this.threadName);
        return t;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }
}