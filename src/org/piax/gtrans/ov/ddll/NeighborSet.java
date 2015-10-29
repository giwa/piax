/*
 * NeighborSet.java - NeighborSet implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NeighborSet.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.piax.gtrans.RPCException;
import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class for implementing a (left) neighbor set.
 */
public class NeighborSet {
    /*
     * (NEIGHBOR_SET_SIZE = 4 case)
     * 
     * inserting case
     *   0 1 2   4 5 (initially 4 knows {2 1 0 5})
     *   0 1 2 3 4 5 (3 is inserted)
     *               3 recvs {2 1 0 5} via SetRAck
     *               3 sends {3 2 1 0} to node 4
     *               4 sends {4 3 2 1} to node 5
     *
     * deleting case
     *   0 1 2 3 4 5 (initially 4 knows {3 2 1 0})
     *   0 1 2   4 5 (3 is deleted)
     *               when 2 receives SetR, 2 sends {2 1 0 5} to node 4
     *               4 sends {4 2 1 0} to node 5
     *
     * failure-recovery case
     *   0 1 2 3  (initially 3 knows {2 1 0}
     *  [0]1 2 3  (0 is failed)
     *            when 3 receives SetR, 3 sends {3 2} to node 1
     *            (node 1 and 0 should be removed from the forwarding set)
     * 
     * TODO: improve multiple keys on a single node case
     *   0 1 2 3 4 5 6 (0=1=2, 3=4=5, 6)
     *               5, 4 and 3 know {2 6}
     *               2, 1 and 0 know {6 5}
     *               6 knows {5 2}
     *   0 1 2 3 4 5 6 (0=2=4, 1=3=5, 6)
     *               Node0=2=4 knows {Node6 Node1=3=5}
     *               Node1=3=5 knows {Node0=2=4 Node6}
     */
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(NeighborSet.class);

    public static int NEIGHBOR_SET_SIZE = 6;
    final int capacity;
    final private Link me;
    final private DdllKey key;
    final private NodeManager manager;
    private static KeyComparator keyComp = KeyComparator.getInstance();
    // 最後に送った右ノード
    private Link prevRight;
    // 最後に prevRight に送った集合
    private Set<Link> prevset;

    /**
     * 左側近隣ノード集合．
     * 例えばノード4のleftNbrSetは {4, 3, 0, 95, 90, 88}のような順序で並ぶ．
     */
    ConcurrentSkipListSet<Link> leftNbrSet = new ConcurrentSkipListSet<Link>(
            new LinkComparator());
    
    @SuppressWarnings("serial")
    class LinkComparator implements Comparator<Link>, Serializable {
        // o1 の方が前に来るならば負の数を返す．
        public int compare(Link o1, Link o2) {
            int c = keyComp.compare(o2.key, o1.key);
            if (c == 0) {
                return 0;
            }
            if (keyComp.compare(o1.key, key) <= 0) {
                // o1 < key
                if (keyComp.compare(o2.key, key) <= 0) {
                    return c;
                }
                // o1 < key && o2 > key
                return -1;  // o1の方が前
            }
            if (keyComp.compare(o2.key, key) <= 0) {
                // o1 > key && o2 < key case
                return +1;  // o2の方が前
            }
            return c;
        }
    }

    NeighborSet(Link me, NodeManager manager, int capacity) {
        this.me = me;
        this.key = me.key;
        this.manager = manager;
        this.capacity = capacity;
    }

    NeighborSet(Link me, NodeManager manager) {
        this(me, manager, NEIGHBOR_SET_SIZE);
    }

    @Override
    public String toString() {
        return leftNbrSet.toString();
    }

    /**
     * 指定されたノード集合を近隣ノード集合とする．
     * マージするのではなく，入れ替えることに注意．
     * @param nbrs 新しい近隣ノード集合
     */
    void set(Set<Link> nbrs) {
        logger.debug("set: before {}", leftNbrSet);
        leftNbrSet.clear();
        leftNbrSet.addAll(nbrs);
        logger.debug("set: after {}", leftNbrSet);
    }

    void setPrevRightSet(Link prevRight, Set<Link> nset) {
        this.prevRight = prevRight;
        this.prevset = nset;
    }

    /**
     * 近隣ノード集合に n を追加する．
     * ただし，近隣ノード集合の大きさが capacity を超える場合には n が入らないこともある．
     * 
     * @param n 追加するノード
     */
    void add(Link n) {
        addAll(Collections.singleton(n));
    }

    void addAll(Collection<Link> nodes) {
        NavigableSet<Link> propset = leftNbrSet.clone();
        propset.addAll(nodes);
        while (capacity > 0 && propset.size() > capacity) {
            propset.remove(propset.last());
        }
        set(propset);
    }

    /**
     **/
    void removeNode(Link removed) {
        leftNbrSet.remove(removed);
        logger.debug("removeNode: {} is removed", removed);
    }
    
    /**
     **/
    void removeNodes(Collection<Link> toRemove) {
        leftNbrSet.removeAll(toRemove);
    }

    /**
     * 左ノードから新しい近隣ノード集合を受信した場合に呼ばれる．
     *
     * @param newset    左ノードから受信した近隣ノード集合
     * @param right     次に転送する右ノード
     * @param limit     限界
     */
    synchronized void receiveNeighbors(Set<Link> newset, Link right,
            DdllKey limit) {
        set(newset);
        sendRight(right, limit);
    }

    /**
     * 右ノードに対して近隣ノード集合を送信する．
     * 右ノードが limit と等しいか，limit を超える場合は送信しない．
     * 送信する近隣ノード集合は，自ノードの近隣ノード集合に自分自身を加えたものである．
     * 
     * @param right     右ノード
     * @param limit     送信する限界キー．
     */
    synchronized void sendRight(Link right, DdllKey limit) {
        // 以下の if 文はコメントアウトしてある．
        // 10A-20B-30B-40C (数値はkey, アルファベットは物理ノードを表す)
        // このとき，20の近隣ノード集合を30に送っておかないと，40が必要な情報を得られないため．
        //if (right.addr.equals(me.addr)) {
        //return;
        //}

        if (Node.isOrdered(key, limit, right.key)) {
            logger.debug("right node {} reached to the limit {}", right, limit);
            return;
        }

        Set<Link> propset = computeSetForRNode(true, right);
        if (right.equals(prevRight)) {
            logger.debug("me =  {}", me);
            logger.debug("propset =  {}", propset);
            logger.debug("prevset =  {}", prevset);
            if (propset.equals(prevset)) {
                return;
            }
        } else {
            logger.debug("right = {}, prevRight = {}", right, prevRight);
        }

        // return if neighbor set size == 0 (for experiments)
        if (propset.size() == 0) {
            return;
        }

        // propagate to the immediate right node
        logger.debug("propagate to right (node = {}, set = {})", right, propset);
        NodeManagerIf stub = manager.getStub(right.addr);
        try {
            stub.propagateNeighbors(right.key, propset, limit);
        } catch (RPCException e) {
            logger.info("", e);
        }
        prevRight = right;
        prevset = propset;
    }

    /**
     * 右ノードに送信するノード集合を計算する．
     * 
     * @param right 右ノード
     * @return 右ノードに送信するノード集合．
     */
    Set<Link> computeSetForRNode(boolean addMe, Link right) {
        NavigableSet<Link> propset = leftNbrSet.clone();
        if (addMe) {
            propset.add(me);
        }
        // propsetから，右ノードと同じノードと，それ以降のノードを削除する (if any)
        propset = propset.headSet(right, false);
        /*
        // 最後に送った集合と近いほうから比較し，i(>=0)番目のノードが異なるとする．
        // i >= NEIGHBOR_SET_SIZE ならばなにもしない．
        // i < IMMED_PROP_THRESHOLD ならば直ぐに右ノードに通知する．
        // i >= IMMED_PROP_THRESHOLD ならば，一定時間後に右ノードに通知するようにタイマをセットする．
        Iterator<Link> it1 = prevset.iterator();
        Iterator<Link> it2 = propset.iterator();
        int i = 0;
        for (; it1.hasNext() && it2.hasNext(); i++) {
            Link link1 = it1.next();
            Link link2 = it2.next();
            if (!link1.equals(link2)) {
                break;
            }
        }
        if (i >= NEIGHBOR_SET_SIZE) {
            return;
        }*/
        while (propset.size() > capacity) {
            propset.remove(propset.last());
        }
        // create a copy of propset.  propset has a reference to our
        // customized Comparator, which has a reference to NeighborSet.
        Set<Link> copy = new HashSet<Link>();
        copy.addAll(propset);
        return copy;
    }

    //    public static void main(String[] args) {
//        Endpoint loc
//            = new TcpLocator(new InetSocketAddress("127.0.0.1", 8080));
//        Link l = new Link(loc, new DdllKey(10, PeerId.MINUS_INFTY));
//        NeighborSet ln = new NeighborSet(l, null);
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(20, PeerId.MINUS_INFTY)));
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(5, PeerId.MINUS_INFTY))); 
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(3, PeerId.MINUS_INFTY)));
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(21, PeerId.MINUS_INFTY)));
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(8, PeerId.MINUS_INFTY)));
//        ln.leftNbrSet.add(new Link(loc, new DdllKey(1, PeerId.MINUS_INFTY)));
//        System.out.println(ln.leftNbrSet);
//    }
}
