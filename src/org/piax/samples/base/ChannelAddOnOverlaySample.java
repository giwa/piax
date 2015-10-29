package org.piax.samples.base;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.ChannelAddOnTransport;

/**
 * Overlay（ここではMSkipGraph）をChannelTransportに拡張した上で、
 * ChannelTransportの提供するsend, newChannel, channel.sendを使う簡単なサンプルプログラム
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public class ChannelAddOnOverlaySample {

    static void printf(String f, Object... args) {
        System.out.printf(f, args);
    }

    static void sleep(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {
        }
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
     * BaseTransportのsenderまたはreceiverとなるアプリクラス
     */
    static class App implements TransportListener<PeerId>, ChannelListener<PeerId> {
        final ObjectId appId;

        App(String id) {
            appId = new ObjectId(id);
        }

        public boolean onAccepting(Channel<PeerId> ch) {
            printf("(%s) new ch-%d accepted from %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId());
            return true;
        }

        public void onClosed(Channel<PeerId> ch) {
            printf("(%s) ch-%d closed via %s%n", appId, ch.getChannelNo(),
                    ch.getRemoteObjectId());
        }

        public void onFailure(Channel<PeerId> ch, Exception cause) {
        }

        public void onReceive(Channel<PeerId> ch) {
            // acceptしたchannelの場合だけ反応させる
            if (ch.isCreatorSide()) return;

            // 受信したメッセージをそのまま返送する
            String msg = (String) ch.receive();
            printf("(%s) ch-%d received msg from %s: %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId(), msg);
            printf("(%s) reply to %s via ch-%d: %s%n", appId,
                    ch.getRemoteObjectId(), ch.getChannelNo(), msg);
            try {
                ch.send(msg);
            } catch (IOException e) {
                System.err.println(e);
            }
        }

        public void onReceive(Transport<PeerId> trans, ReceivedMessage rmsg) {
            printf("(%s) received msg from %s: %s%n", appId,
                    rmsg.getSender(), rmsg.getMessage());
        }
    }

    /**
     * main code
     * 
     * @param args
     */
    public static void main(String[] args) throws IOException, IdConflictException {
        Net ntype = Net.TCP;
        printf("- start -%n");
        printf("- locator type: %s%n", ntype);

        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // sender, receiverとなるAppを生成する
        App app1 = new App("app1");
        App app2 = new App("app2");

        // BaseTransportを生成する
        ChannelTransport<?> bt1, bt2;
        bt1 = p1.newBaseChannelTransport(genLocator(ntype, "localhost", 10001));
        bt2 = p2.newBaseChannelTransport(genLocator(ntype, "localhost", 10002));
        
        // MSkipGraphを生成する
        Overlay<PeerId, PeerId> sg1, sg2;
        sg1 = new MSkipGraph<PeerId, PeerId>(bt1);
        sg2 = new MSkipGraph<PeerId, PeerId>(bt2);
        sg1.join(bt1.getEndpoint());
        sg2.join(bt1.getEndpoint());
        
        // MSkipGraphをchanneltransportに拡張する
        ChannelTransport<PeerId> tr1, tr2;
        tr1 = new ChannelAddOnTransport<PeerId>(sg1);
        tr2 = new ChannelAddOnTransport<PeerId>(sg2);
        
        // BaseTransportに、Listenerをセットする
        tr1.setListener(app1.appId, app1);
        tr2.setListener(app2.appId, app2);
        tr1.setChannelListener(app1.appId, app1);
        tr2.setChannelListener(app2.appId, app2);

        // 文字列を送信する
        tr1.send(app1.appId, app2.appId, tr2.getEndpoint(), "123456");
        tr2.send(app2.appId, app1.appId, tr1.getEndpoint(), "654321");
        Channel<PeerId> ch = tr1.newChannel(app1.appId, app2.appId, tr2.getEndpoint());
        ch.send("abcdefg");
        printf("(%s) wait for receiving reply message%n", app1.appId);
        String rep = (String) ch.receive(1000);
        printf("(%s) ch-%d received reply from %s: %s%n", app1.appId,
                ch.getChannelNo(), ch.getRemoteObjectId(), rep);

        ch.close();
        sleep(100);
//        tr1.fin();
//        tr2.fin();
//        sg1.fin();
//        sg2.fin();
//        bt1.fin();
//        bt2.fin();
        p1.fin();
        p2.fin();
        printf("- end -%n");
    }
}
