package org.yamcs.commanding;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.yamcs.parameter.ParameterValue;
import org.yamcs.xtce.CommandVerifier;

abstract class Verifier {
    final protected CommandVerifier cv;
    final protected CommandVerificationHandler cvh;
    final ScheduledThreadPoolExecutor timer;

    enum State {
        NEW, RUNNING, OK, NOK, TIMEOUT, DISABLED, CANCELLED
    };

    volatile State state = State.NEW;

    Verifier nextVerifier;

    Verifier(CommandVerificationHandler cvh, CommandVerifier cv) {
        this.cv = cv;
        this.cvh = cvh;
        this.timer = cvh.timer;
    }

    void start() {
        state = State.RUNNING;
        doStart();
    }

    void timeout() {
        if (state != State.RUNNING) {
            return;
        }
        state = State.TIMEOUT;
        doCancel();
        cvh.onVerifierFinished(this, null);
    }

    void cancel() {
        if (state != State.RUNNING && state != State.NEW) {
            return;
        }
        state = State.CANCELLED;
        doCancel();
        cvh.onVerifierFinished(this, null);
    }

    void finished(boolean result, String failureReason) {
        if (state != State.RUNNING) {
            return;
        }
        state = result ? State.OK : State.NOK;
        cvh.onVerifierFinished(this, failureReason);
    }

    void finishOK() {
        finished(true, null);
    }

    void finishNOK() {
        finished(false, null);
    }

    abstract void doStart();

    /**
     * Called to cancel the verification in case it didn't finish in the expected time.
     */
    abstract void doCancel();

    /**
     * Called when a command history parameter (an entry in the command history) is received The parameter name is set
     * to /yamcs/cdmHist/&lt;key&gt; where the key is the command history key.
     * 
     * 
     * @param pv
     */
    public void updatedCommandHistoryParam(ParameterValue pv) {
    }

    public State getState() {
        return state;
    }
}
