package org.piax.samples.rpc;

import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;

/**
 * RPCTargetのRPC対象メソッドの宣言を与えるinterface
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public interface RPCTargetIf extends RPCIf {
    // このメソッドはremoteからは呼び出せない
    void dummy0();
    
    @RemoteCallable(Type.ONEWAY)
    void dummy1() throws RPCException;
    
    @RemoteCallable
    void nullPo() throws RPCException;

    // TYPE.SYNC は default値なので省略して良い
    @RemoteCallable(Type.SYNC)
    int sum(int from, int to) throws RPCException, DummyException;
    
    @RemoteCallable
    String echo(String... msg) throws RPCException;
    
    // 同期型でmsecの間スリープする呼び出し。タイムアウトのテストで使う。
    @RemoteCallable
    void longSleep(int msec) throws RPCException;
    
    @RemoteCallable(Type.ONEWAY)
    void longSleepAsync(int msec) throws RPCException;
}
