package org.piax.samples.rpc;

import java.io.IOException;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;

/**
 * RPCの呼び出し対象となるtargetクラスの定義。
 * RPCの対象となるメソッドは、RPCTargetIfによって宣言されるものとする。
 * この例では、RPCInvokerのサブクラスを作り、そこに呼び出したいメソッドを追加する方法を示す。
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public class RPCTarget<E extends PeerLocator> extends
        RPCInvoker<RPCTargetIf, E> implements RPCTargetIf {
    
    public static final TransportId transId = new TransportId("invo");
    
    static void printf(String format, Object... args) {
        System.out.printf(format, args);
    }
    
    static void sleep(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {}
    }

    public RPCTarget(ChannelTransport<E> trans) throws IOException, IdConflictException {
        super(transId, trans);
    }

    public void dummy0() {
        printf("dummy0 called%n");
    }

    public void dummy1() throws RPCException {
        printf("dummy1 called%n");
    }

    public void nullPo() throws RPCException {
        printf("nullPo called%n");
        throw new NullPointerException();
    }

    public int sum(int from, int to) throws DummyException {
        printf("sum called%n");
        if (to < from) {
            throw new DummyException();
        }
        int sum = 0;
        for (int i = from; i < to; i++)
            sum += i;
        return sum;
    }

    public String echo(String... msgs) {
        printf("echo called%n");
        StringBuilder sb = new StringBuilder();
        for (String s : msgs) {
            sb.append(s);
        }
        return sb.toString();
    }

    public void longSleep(int msec) {
        printf("longSleep called%n");
        sleep(msec);
        printf("longSleep exit%n");
    }

    public void longSleepAsync(int msec) {
        printf("longSleepAsync called%n");
        sleep(msec);
        printf("longSleepAsync exit%n");
    }
}
