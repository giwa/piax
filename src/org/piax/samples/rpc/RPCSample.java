package org.piax.samples.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;

/**
 * RPCを使う簡単なサンプルプログラム
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public class RPCSample {

    static void printf(String format, Object... args) {
        System.out.printf(format, args);
    }
    
    static void sleep(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {}
    }

    enum Net {
        EMU, UDP, TCP
    }

    @SuppressWarnings("unchecked")
    static <E extends PeerLocator> E genLocator(Net net, String host, int port) {
        PeerLocator loc;
        switch (net) {
        case EMU:
            loc = new EmuLocator(port);
            break;
        case UDP:
            loc = new UdpLocator(new InetSocketAddress(host, port));
            break;
        case TCP:
            loc = new TcpLocator(new InetSocketAddress(host, port));
            break;
        default:
            loc = null;
        }
        return (E) loc;
    }

    /**
     * main code
     * 
     * @param args
     */
    public static <E extends PeerLocator> void main(String[] args) {
        System.out.println("-start-");
        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // BaseTransportを生成する
        Net ntype = Net.UDP;
        ChannelTransport<E> tr1, tr2;
        try {
            tr1 = p1.newBaseChannelTransport(
                    RPCSample.<E>genLocator(ntype, "localhost", 10001));
            tr2 = p2.newBaseChannelTransport(
                    RPCSample.<E>genLocator(ntype, "localhost", 10002));
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }

        // RPCInvokerを生成する
        RPCTarget<E> invo1, invo2;
        try {
            invo1 = new RPCTarget<E>(tr1);
            invo2 = new RPCTarget<E>(tr2);
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }
        
        // p1でStubを作って、p2のInvo2のメソッド呼び出しをする
        RPCTargetIf stub = invo1.getStub(tr2.getEndpoint());
//        RPCTargetIf wrongStub = invo1.getStub(p2);
        RPCTargetIf localStub = invo1.getStub(tr1.getEndpoint());
        
//        try {
//            // 通信エラーが発生する
//            wrongStub.echo("sss");
//        } catch (RPCException e) {
//            System.out.println(e);
//        }
        
        try {
            // callに失敗する
            stub.dummy0();
        } catch (Exception e) {
            // IllegalRPCAccessExceptionをキャッチ
            System.out.println(e);
        }

        // こちらは成功する
        localStub.dummy0();
        
        try {
            // callに失敗する
            stub.nullPo();
        } catch (RPCException e) {
            System.out.println(e);
        } catch (Exception e) {
            // NullPointerExceptionをここでキャッチ
            System.out.println(e);
        }
        
        //-- RPCを使う上での標準パターン
        try {
            int sum = stub.sum(10, 20);
            System.out.println("ans = " + sum);
        } catch (DummyException e) {
            // interfaceで定義した例外
            // sum(20, 10)と呼び出した場合
            System.out.println(e);
        } catch (RPCException e) {
            // RPC呼び出しで起こりうる例外
            // NetworkTimeoutException
            // SocketExceptionなどのIOExceptionがcauseとなるNoSuchPeerException
            // NoSuchRemoteObjectExceptionなど
            System.out.println(e);
        }
        
        try {
            stub.dummy1();
            System.out.println("echo ret: " + stub.echo("1", "2", "3"));
            stub.longSleepAsync(1000);
        } catch (RPCException e) {
            System.out.println(e);
        }
    
        // 終了
        invo1.fin();
        invo2.fin();
        p1.fin();
        p2.fin();
        System.out.println("-end-");
    }
}
