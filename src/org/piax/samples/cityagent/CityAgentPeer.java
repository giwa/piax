package org.piax.samples.cityagent;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.piax.agent.AgentId;
import org.piax.agent.AgentInstantiationException;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.attribs.RowData;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.ChannelAddOnTransport;

/**
 * 都市の情報（都市名、人口、緯度経度）を持つAgentを使って様々な検索を行うサンプルプログラム
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public class CityAgentPeer {

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

    //--- test data
    // attributes
    static String[] attribs = new String[] {"city", "pop", "loc"};
    
    // for p0
    static Object[][] rowData0 = new Object[][] {
        {"奈良市", 366528, new Location(135.836105, 34.679359)},
        {"京都市", 1474473, new Location(135.754807, 35.009129)},
    };
    
    // for p1
    static Object[][] rowData1 = new Object[][] {
        {"舞鶴市", 88681, new Location(135.332901, 35.441528)},
        {"大阪市", 2666371, new Location(135.496505, 34.702509)},
        {"和歌山市", 369400, new Location(135.166107, 34.221981)},
        {"神戸市", 1544873, new Location(135.175479, 34.677484)},
        {"姫路市", 536338, new Location(134.703094, 34.829361)},
        {"津市", 285728, new Location(136.516800, 34.719109)},
    };
    
    // dcond
    static String[] dconds = new String[] {
        "pop in [1000000..10000000)",
        "pop in [0..300000) and loc in rect(135, 34.5, 1, 1)",
        "pop eq maxLower(1000000)",
        "pop in lower(1000000, 3)",
    };

    public final Peer peer;
    public final ChannelTransport<?> bt;
    public final ChannelTransport<?> tr;
    public final Overlay<Destination, ComparableKey<?>> sg;
    public final LLNet llnet;
    public AgentHomeImpl home;
    
    public CityAgentPeer(Peer peer, PeerLocator locator)
            throws IdConflictException, IOException {
        this.peer = peer;
        
        // init overlays
        bt = peer.newBaseChannelTransport(locator);
      //sg = new MSkipGraph<Destination, ComparableKey<?>>(bt);
        sg = new Suzaku<Destination, ComparableKey<?>>(bt);
        llnet = new LLNet(sg);
        // RPC用transportとして、sgを使う
        tr = new ChannelAddOnTransport<PeerId>(sg);
        
        // create table and CombinedOverlay
        home = new AgentHomeImpl(tr, new File("."));
        
        // declare attribute and bind overlay
        home.declareAttrib("city");
        home.declareAttrib("pop", Integer.class);
        home.declareAttrib("loc", Location.class);
        try {
            home.bindOverlay("pop", sg.getTransportIdPath());
            home.bindOverlay("loc", llnet.getTransportIdPath());
        } catch (NoSuchOverlayException e) {
            System.out.println(e);
        } catch (IncompatibleTypeException e) {
            System.out.println(e);
        }
    }
    
    public void leave() throws IOException {
        sg.leave();
    }
    
    public void fin() {
        home.fin();
        peer.fin();
//        llnet.fin();
//        sg.fin();
//        tr.fin();
//        bt.fin();
    }

    public void join(Endpoint seed) throws IOException {
        sg.join(seed);
    }
    
    public void newAgentsWithAttribs(String[] attribs, Object[][] rowData) {
        for (int i = 0; i < rowData.length; i++) {
            try {
                AgentId agId = home.createAgent(CityAgent.class, "ag" + i);
                CityAgentIf stub = home.getStub(CityAgentIf.class,agId);
                for (int j = 0; j < rowData[i].length; j++) {
                    Object val = rowData[i][j];
                    if (val != null)
                        stub.setAttrib(attribs[j], val);
                }
            } catch (AgentInstantiationException e) {
                System.out.println(e);
            } catch (IncompatibleTypeException e) {
                System.out.println(e);
            }
        }
    }
    
    public void printTable() {
        System.out.println(home.comb.table);
    }

    public void dumpTable() {
        for (String att : home.comb.getDeclaredAttribNames()) {
            System.out.println(home.comb.table.getAttrib(att));
        }
        for (RowData row : home.comb.getRows()) {
            System.out.println(row);
        }
    }

    public void printStat() {
        printf("** peer %s status**%n", peer.getPeerId());
        printf("-> agIds:%s%n", home.getAgentIds());
        printf("-> sg keys=%s%n", sg.getKeys(AgentHomeImpl.COMBINED_OV_ID));
        printTable();
    }
    
    /**
     * main code
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {

        printf("-- Agent discoveryCall and RPC test --%n");
        Peer p0 = Peer.getInstance(new PeerId("p0"));
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        
        printf("-- init peers --%n");
        CityAgentPeer root = new CityAgentPeer(p0, genLocator(Net.EMU, "localhost", 10000));
        CityAgentPeer peer = new CityAgentPeer(p1, genLocator(Net.EMU, "localhost", 10001));
        
        printf("-- join peers --%n");
        root.join(root.sg.getBaseTransport().getEndpoint());
        peer.join(root.bt.getBaseTransport().getEndpoint());
        peer.dumpTable();
        
        printf("-- new agent and set attributes on each peer --%n");
        root.newAgentsWithAttribs(attribs, rowData0);
        peer.newAgentsWithAttribs(attribs, rowData1);
        printf("-- print internal table on each peer --%n");
        root.printTable();
        peer.printTable();
        
        printf("-- invoke discoveryCall --%n");
        for (int i = 0; i < dconds.length; i++) {
            Object[] ret = root.home.discoveryCall(dconds[i], "getCityName");
            printf("\"%s\" => %s%n", dconds[i], Arrays.toString(ret));
        }

        Endpoint dst = peer.tr.getEndpoint();
        printf("-- invoke normal RPC to %s --%n", dst);
        for (AgentId dstId : peer.home.getAgentIds()) {
            CityAgentIf stub = root.home.getStub(CityAgent.class, dstId, dst);
            printf("result of getCityName() call to %s: %s%n", dstId, stub.getCityName());
        }
        
        printf("%n-- test fin --%n");
        sleep(100);
        peer.leave();
        root.leave();
        sleep(100);
        peer.fin();
        root.fin();
    }
}
