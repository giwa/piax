/*
 * RingVNode.java - A virtual node of ring overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.gtrans.ov.ring;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.piax.common.Endpoint;
import org.piax.common.subspace.CircularRange;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.NodeObserver;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ring.TemporaryIOException;
import org.piax.gtrans.ov.ring.UnavailableException;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a virtual node in simple ring network.
 * 
 * @param <E> the type of Endpoint in the underlying network.
 */
public class RingVNode<E extends Endpoint> implements NodeObserver {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RingVNode.class);

    /** mode of DdllRingVNode */
    public static enum VNodeMode {
        /** not inserted */
        OUT, INSERTING,
        /** inserted */
        INSERTED,
        /** deleting */
        DELETING, DELETED
    };

    public final static int DEFAULT_DDLL_CHECK_PERIOD = 30*1000;
    public final static int NUMBER_OF_DDLL_RETRY = 100;
    public final static int DDLL_RETRY_INTERVAL = 100;

    protected final RingManager<E> manager;
    final Comparable<?> rawkey;
    protected final DdllKey key;
    protected Node ddllNode;

    protected VNodeMode mode = VNodeMode.OUT;

    protected static int getCheckPeriod() {
        return DEFAULT_DDLL_CHECK_PERIOD;
    }

    /**
     * create a RingVNode instance.
     * 
     * @param manager       the p2p network instance that manages this node
     * @param rawkey    the key
     */
    public RingVNode(RingManager<E> rman, Comparable<?> rawkey) {
        this.manager = rman;
        this.rawkey = rawkey;
        this.key = new DdllKey(rawkey, new UniqId(rman.peerId));
        this.ddllNode = rman.manager.createNode(this.key, this);

        /* register instance for debug */
        //        synchronized (DdllRingVNode.class) {
        //            sgnodes.add(this);
        //        }
    }

    public Node getDdllNode() {
        return ddllNode;
    }

    public VNodeMode getMode() {
        return mode;
    }

    public Link getLocalLink() {
        return ddllNode.getMyLink();
    }

    public Link getSuccessor() {
        return ddllNode.getRight();
    }

    public Link getPredecessor() {
        return ddllNode.getLeft();
    }


    // Override me 
    public Link[] getAllLinks() {
        Link[] links =
                new Link[] { getPredecessor(), getLocalLink(), getSuccessor()};
        return links;
    }

    /* instances for debug */
    //    private static ArrayList<DdllRingVNode<?>> sgnodes = new ArrayList<DdllRingVNode<?>>();

    /* dump nodes for debug */
    //    static public synchronized void dump() {
    //        FileWriter fw = null;
    //        String fn = "DdllRingVNode-dump-" + System.currentTimeMillis() + ".txt";
    //        logger.warn("DdllRingVNode dump to{}", fn);
    //        try {
    //            fw = new FileWriter(fn);
    //        } catch (IOException e) {
    //            logger.error("file open", e);
    //            return;
    //        }
    //        try {
    //            for (DdllRingVNode<?> sgnode : sgnodes) {
    //                fw.write("sgnode " + sgnode.rawkey + "\n");
    //                sgnode.rtLockW();
    //                for (Tile tile : sgnode.table) {
    //                    fw.write(tile.toString() + "\n");
    //                }
    //                sgnode.rtUnlockW();
    //            }
    //        } catch (IOException e) {
    //            logger.error("", e);
    //        } finally {
    //            try {
    //                fw.close();
    //            } catch (IOException e) {
    //                logger.error("file close", e);
    //            }
    //        }
    //    }

    /*
     * reader writer locks
     */
    protected void rtLockR() {
        manager.rtLockR();
    }

    protected void rtUnlockR() {
        manager.rtUnlockR();
    }

    protected void rtLockW() {
        manager.rtLockW();
    }

    protected void rtUnlockW() {
        manager.rtUnlockW();
    }

    /**
     * insert a key into a ring.
     * 
     * @param introducer 既に挿入済みのノード
     * @return 成功したらtrue
     * @throws UnavailableException introducerにkeyが存在しない
     * @throws IOException introducerとの通信でエラー or insertion failure
     */
    protected boolean addKey(E introducer) throws UnavailableException,
            IOException {
        logger.trace("ENTRY:");
        logger.debug("addKey {}, seed: {}", rawkey, introducer);
        if (rawkey == null) {
            throw new IllegalArgumentException("null key specified");
        }
        for (int i = 0; i < NUMBER_OF_DDLL_RETRY; i++) {
            try {
                InsertPoint pos = manager.findImmedNeighbors(introducer, key,
                		null // XXX TransOptions
                		);
                if (pos == null) {
                    ddllNode.insertAsInitialNode();
                    mode = VNodeMode.INSERTED;
                    return true;
                }
                mode = VNodeMode.INSERTING;
                boolean inserted = false;
                if (ddllNode.isBetween(pos.left.key, pos.right.key)) {
                    inserted = ddllNode.insert(pos);                		
                } else {
                    // XXX pos に到達したあと，pos.left, pos.right を取り出すまでの間に
                		// 別のノードが挿入された。検索からやりなおす。
                    logger.debug(rawkey + ": not ordered: " + pos);
                    // wrap around境界にひっかかると全ノードをトラバースする可能性があるので，
                    // 下記は実行せず、失敗とし、再度挿入場所を検索する。
                   // inserted = ddllNode.insert(pos.left, 1);
                }
                if (inserted) {
                    logger.debug("addKey(key={}): DDLL insertion succeeded (i={})",
                            key, i);
                    mode = VNodeMode.INSERTED;
                    return true;
                }
                logger.debug("addKey(key={}): DDLL insertion failed (i={})", key, i);
                mode = VNodeMode.OUT;
                introducer = (E) pos.left.addr;
            } catch (TemporaryIOException e) {
                System.err.println("addKey(key=" + rawkey + ", got " + e);
                logger.debug("addKey(key={}): got {}", rawkey, e);
            }
            try {
            		Thread.sleep((long)(DDLL_RETRY_INTERVAL + DDLL_RETRY_INTERVAL * Math.random()));
            } catch (InterruptedException e) {
            }
        }
        logger.debug("addKey(key={}): DDLL insertion failed (final)", key);
        return false;
    }

    protected boolean removeKey() {
        rtLockW();
        if (mode != VNodeMode.INSERTED) {
            rtUnlockW();
            return false;
        }
        mode = VNodeMode.DELETING;
        rtUnlockW();
        ddllNode.delete(NUMBER_OF_DDLL_RETRY);
        rtLockW();
        mode = VNodeMode.OUT;
        rtUnlockW();
        return true;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        Comparable<?> k = rawkey;
        buf.append("key=" + k);
        if (k != null) {
            buf.append(" (" + k.getClass().getCanonicalName() + "), ");
        }
        buf.append("mode=" + mode + "\n");
        buf.append(toStringRoutingTable());
        return buf.toString();
    }

    // Override me 
    public String toStringRoutingTable() {
        return ddllNode.toString();
    }

    public boolean isInserted() {
        return (mode == VNodeMode.INSERTED);
    }

    /**
     * get raw key
     * @return  raw key
     */
    public Comparable<?> getRawKey() {
        return rawkey;
    }

    /**
     * get DdllKey 
     * @return  DdllKey
     */
    public DdllKey getKey() {
        return key;
    }

    public RingManager<E> getManager() {
        return manager;
    }

    public void fixLeftLinks(Link link, List<Link> failedLinks, RQMessage msg,
            List<CircularRange<DdllKey>> failedRanges) {
        logger.debug("fixLeftLinks: link={}, failedLinks={}, msg={}, failedRanges={}", 
                link, failedLinks, msg, failedRanges);
    }

    /*
     * DDLL NodeObserver interfaces
     */

    @Override
    public void onRightNodeChange(Link prevRight, Link newRight, Object payload) {
        logger.debug("{}: rightNodeChanged from {} to {}, {}", key, prevRight,
                newRight, payload);
    }

    @Override
    public void payloadNotSent(Object payload) {
        logger.debug("{}: payloadNotSent received illgal payload {}", key,
                payload);
    }

    @Override
    public boolean onNodeFailure(Collection<Link> failedLinks) {
        logger.debug("onNodeFailure: {}", failedLinks);
        return true; // let DDLL to fix this link
    }

    @Override
    public List<Link> suppplyLeftCandidatesForFix() {
        return null;
    }
}
