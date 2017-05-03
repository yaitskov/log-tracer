package org.dan.tracer;

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

public class AwaitableVar<T> {
    private static final Logger logger = getLogger(AwaitableVar.class);

    private final String label;
    private Thread waiter;
    private T value;

    public AwaitableVar(String label) {
        this.label = label;
    }

    public synchronized T peek() {
        return value;
    }

    public synchronized void set(T t) {
        if (value != null) {
            throw new IllegalArgumentException(
                    "Attempt to reassign label ["
                            + label + "] with value [" + t + "]");
        }
        value = t;
        logger.info("Var {} is set with {}", label, t);
        notify();
    }

    public synchronized T get() throws InterruptedException {
        if (waiter != null) {
            throw new IllegalStateException("avar "
                    + label + " is already awaited by thread "
                    + waiter);
        }
        waiter = Thread.currentThread();
        try {
            while (value == null) {
                wait();
            }
            logger.info("Var {} is read with {}", label, value);
            return value;
        } finally {
            waiter = null;
        }
    }

    @Override
    public String toString() {
        if (waiter == null) {
            if (value == null) {
                return "avar " + label + " is not set";
            } else {
                return "avar " + label + " is set to " + value;
            }
        } else {
            return "avar " + label + " is awaited by " + waiter;
        }
    }
}
