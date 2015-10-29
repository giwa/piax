/*
 * Node.java - Node implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Node.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.util.KeyComparator;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a DDLL node.
 * <p>
 * this class contains an implementation of DDLL protocol.
 */
public class Node {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    static String THREAD_NAME_PREFIX = "ddll-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    /*--- constants ---*/
    public static boolean GOD_MODE = false;
    public static boolean COMPARE_WITH_GOD = false;

    public static int DEFAULT_TIMEOUT = 20000;
    public static int SETR_TIMEOUT = DEFAULT_TIMEOUT;
    public static int DELETE_OP_TIMEOUT = DEFAULT_TIMEOUT;
    public static int SEARCH_OP_TIMEOUT = DEFAULT_TIMEOUT;

    /**
     * timeout for a response of GetStat message in msec. if the left node does
     * not respond within this period, the left node is considered failed. TODO:
     * change timeout adaptively
     */
    public static int GETSTAT_OP_TIMEOUT = 20000;
    public static final int INS_DEL_RETRY_INTERVAL_BASE = 200;
    
    /**
     * minimum interval of fixing
     * To avoid rush of fixing, make minimum interval between fixing.
     */
    public static int MIN_FIX_INTERVAL = 500;

    /**
     * fix処理を定期実行するための基準となる周期 (msec)
     */
    public static int DEFAULT_CHECK_PERIOD = 10000;

    /**
     * DDLL node state
     */
    public enum Mode {
        OUT, INS, INSWAIT, IN, DEL, DELWAIT, GRACE,
    };

    /**
     * the state of the link repair procedure.
     */
    static enum FIX_STATE {
        /** idle */
        WAITING,
        /** checking left links (sending getStat message) */
        CHECKING,
        /** fixing left links (sending SetR message) */
        FIXING
    }

    private static KeyComparator keyComp = KeyComparator.getInstance();

    final NodeManager manager;
    final NodeObserver observer;
    boolean isOnline = true;
    /** a Link to myself */
    final Link me;
    final DdllKey key;
    volatile Mode mode = Mode.OUT;
    volatile Link left;
    volatile LinkNum lNum;
    volatile Link right;
    volatile LinkNum rNum;
    volatile int ref = 0;
    volatile boolean lastUnrefL = true;
    // the latest sequence number of the SetR message for deletion
    int delReqNo;
    boolean expectDelayedResponse = false;

    private final ReentrantReadWriteLock protoLock =
            new ReentrantReadWriteLock();
    private Condition protoCond = protoLock.writeLock().newCondition();

    private final FutureValues futures = new FutureValues();

    private final Timer stabilizeTimer;
    private volatile FIX_STATE fixState = FIX_STATE.WAITING;
    private TimerTask fixTask;
    /** the period for checking the left link */
    private int checkPeriod = DEFAULT_CHECK_PERIOD;

    /** neighbor node set */
    final NeighborSet leftNbrs;

    private final static boolean TOSTRING_EMBED_NBRS = true;

    /**
     * constructor
     * 
     * @param manager A NodeManager that controls this Node instance.
     * @param observer A NodeObserver that monitors particular events occurred
     *            in this instance (maybe null).
     * @param id A string to identify a linked-list. nodes for different
     *            linked-lists should have different IDs.
     * @param key The key for this DDLL node.
     * @param appData Application-specific data that is used as a part of DdllKey
     *            instance but not used for comparison.
     * @param timer A timer instance used for executing periodic tasks.
     */
    Node(NodeManager manager, NodeObserver observer, String id,
            Comparable<?> key, Object appData, Timer timer) {
        this(manager, observer,
                new DdllKey(key, new UniqId(manager.peerId), id, appData),
                timer);
    }

    /**
     * constructor
     * 
     * @param manager A NodeManager that controls this Node instance.
     * @param observer A NodeObserver that monitors particular events occurred
     *            in this instance (maybe null).
     * @param ddllKey The key for this DDLL node.
     * @param timer A timer instance used for executing periodic tasks.
     */
    Node(NodeManager manager, NodeObserver observer, DdllKey ddllKey,
            Timer timer) {
        this.manager = manager;
        this.observer = observer;
        this.key = ddllKey;
        me = new Link(manager.getLocator(), this.key);
        // setLeft(me);
        setLeftNum(new LinkNum(0, 0));
        // setRight(me);
        // setRightNum(new LinkNum(0, 0));
        leftNbrs = new NeighborSet(me, manager);
        stabilizeTimer = timer;
        online();
    }

    /**
     * activate the protocol.
     */
    public void online() {
        // System.out.println(key + " online() is called");
        isOnline = true;
    }

    /**
     * inactivate the protocol.
     */
    public void offline() {
        //System.out.println(key + " offline() is called");
        protoLock.writeLock().lock();
        isOnline = false;
        fixState = FIX_STATE.WAITING;
        if (fixTask != null) {
            fixTask.cancel();
        }
        protoLock.writeLock().unlock();
    }

    public boolean isOnline() {
        // System.out.println(key + " isOnline = " + isOnline);
        return isOnline;
    }

    // XXX: potential dead lock?  Think!
    public void fin() {
        protoLock.writeLock().lock();
        try {
            offline();
            if (this.left != null) {
                manager.monitor.unregisterNode(this.left, this);
            }
        } finally {
            protoLock.writeLock().unlock();
        }
    }

    /*
     * Getters and Setters
     */
    public Link getMyLink() {
        return (Link) me.clone();
    }

    public DdllKey getKey() {
        return key;
    }

    public Mode getMode() {
        return mode;
    }

    protected void setMode(Mode mode) {
        this.mode = mode;
    }

    public void setCheckPeriod(int period) {
        this.checkPeriod = period;
    }

    protected void setLeft(Link left) {
        if (this.left != null) {
            manager.monitor.unregisterNode(this.left, this);
        }
        this.left = left;
        if (left != null) {
            manager.monitor.registerNode(left, this, checkPeriod);
        }
    }

    protected void setLeftNum(LinkNum lNum) {
        this.lNum = lNum;
    }

    public Link getLeft() {
        protoLock.readLock().lock();
        try {
            if (left == null) {
                return null;
            }
            return (Link) left.clone();
        } finally {
            protoLock.readLock().unlock();
        }
    }

    public Link getRight() {
        protoLock.readLock().lock();
        try {
            if (right == null) {
                return null;
            }
            return (Link) right.clone();
        } finally {
            protoLock.readLock().unlock();
        }
    }

    protected void setRight(Link right) {
        this.right = right;
    }

    protected void setRightNum(LinkNum rNum) {
        this.rNum = rNum;
    }

    protected void setRef(int r) {
        this.ref = r;
    }

    public List<Link> getNeighborSet() {
        List<Link> lst = new ArrayList<Link>(leftNbrs.leftNbrSet);
        return lst;
    }

    @Override
    public String toString() {
        if (TOSTRING_EMBED_NBRS) {
            return "Node[left=" + left + ", key=" + key + ", right=" + right
                    + ", " + mode + "]" + ": lnbr=" + leftNbrs;
        } else {
            return "Node[left=" + left + ", lNum="+lNum+", key=" + key
                    + ", right=" + right + ", rNum="+rNum
                    + ", " + mode + ", " + fixState + "]";
        }
    }

    /**
     * forcibly delete this node without notifying other nodes
     */
    public void reset() {
        protoLock.writeLock().lock();
        try {
            setMode(Mode.OUT);
            setLeft(null);
            setRight(null);
            if (fixTask != null) {
                fixTask.cancel();
            }
            manager.unregisterNode(key);
        } finally {
            protoLock.writeLock().unlock();
        }
    }

    public void lock() {
        protoLock.readLock().lock();
    }

    public void unlock() {
        protoLock.readLock().unlock();
    }

    private NodeManagerIf getStub(Endpoint loc) throws OfflineSendException {
        if (isOnline()) {
            return (NodeManagerIf) manager.getStub(loc);
        }
        throw new OfflineSendException();
    }

    public static boolean isOrdered(Comparable<?> a, Comparable<?> b,
            Comparable<?> c) {
        return keyComp.isOrdered(a, b, c);
    }

    public static boolean isOrdered(Comparable<?> from, boolean fromInclusive,
            Comparable<?> val, Comparable<?> to, boolean toInclusive) {
        boolean rc = keyComp.isOrdered(from, val, to);
        if (rc) {
            if (keyComp.compare(from, val) == 0) {
                rc = fromInclusive;
            }
        }
        if (rc) {
            if (keyComp.compare(val, to) == 0) {
                rc = toInclusive;
            }
        }
        return rc;
    }

    public boolean isBetween(DdllKey a, DdllKey c) {
        return keyComp.isOrdered(a, key, c);
    }

    /**
     * Insert this node as the initial node. This node becomes the first node in
     * the network.
     */
    public void insertAsInitialNode() {
        logger.trace("ENTRY:");
        setLeft(me);
        //setLeftNum(new LinkNum(0, 0));
        setRight(me);
        setRightNum(new LinkNum(0, 0));
        setMode(Mode.IN);
        setRef(1);
        manager.registerNode(this);
        logger.trace("EXIT:");
    }

    public static void initializeConnectedNodes(List<Node> list) {
        logger.trace("ENTRY:");
        logger.debug("List = {}", list);
        for (int i = 0; i < list.size(); i++) {
            Node p = list.get(i);
            Node q = list.get((i + 1) % list.size());
            p.setRight(q.me);
            p.setRightNum(new LinkNum(0, 0));
            q.setLeft(p.me);
            //q.setLeftNum(new LinkNum(0, 0));
            p.setMode(Mode.IN);
            p.setRef(1);
            p.manager.registerNode(p);
            // 物理ノードと近隣ノード集合との関係をよく考える必要がある!
        }
        logger.trace("EXIT:");
    }

    /**
     * insert this node to a linked-list by specifying an introducer node, which
     * is an arbitrary node that has been inserted to the linked-list.
     * <p>
     * note that this method may take O(N) steps for finding proper insertion
     * point and thus, should not be used for large linked-list.
     * 
     * @param introducer some node that has been inserted to a linked-list
     * @param maxRetry max retry count
     * @return true if successfully inserted, false otherwise
     * @throws IllegalStateException thrown if the node is already inserted
     */
    public boolean insert(Link introducer, int maxRetry)
            throws IllegalStateException {
        if (mode != Mode.OUT) {
            throw new IllegalStateException("already inserted");
        }
        manager.registerNode(this);
        boolean rc = false;
        try {
            for (int i = 0; i < maxRetry; i++) {
                if (i != 0) {
                    sleepForRetry(i);
                }
                InsertPoint p;
                try {
                    // XXX: magic number 50!
                    p = findInsertPoint(introducer, key, 50);
                } catch (OfflineSendException e) {
                    logger.info(
                            "insert: findInsertPoint failed as offline while "
                                    + "inserting {}", key);
                    return rc = false;
                }
                if (p == null) {
                    logger.info(
                            "insert: could not find the insertion point for inserting {}, seed={}",
                            key, introducer);
                    // simply return here because if we continue here and if the
                    // introducer has failed, we stuck.
                    return rc = false;
                }
                if (insert0(p)) {
                    return rc = true;
                }
                logger.debug(
                        "insert: insertion failed for some reason.  retry!: {}",
                        this);
            }
        } finally {
            if (!rc) {
                //manager.unregisterNode(key);
                reset();
            }
        }
        return rc;
    }

    /**
     * insert this node to a linked-list by specifying the immediate left and
     * right nodes.
     * <p>
     * note that this method does not retry insertion when failed.
     * 
     * @param pos the insert point, which contains the immediate left and right
     *            node of this node.
     * @return true if successfully inserted, false otherwise
     * @throws IllegalStateException thrown if the node is already inserted
     */
    public boolean insert(InsertPoint pos) throws IllegalStateException {
        if (mode != Mode.OUT) {
            throw new IllegalStateException("already inserted");
        }
        boolean rc = false;
        manager.registerNode(this);
        try {
            rc = insert0(pos);
        } finally {
            if (!rc) {
                //manager.unregisterNode(key);
                reset();
            }
        }
        return rc;
    }

    /**
     * insert a node between two nodes specified by an {@link InsertPoint}
     * instance.
     * 
     * @param p insertion point
     * @return true if successfully inserted, false otherwise
     */
    protected boolean insert0(InsertPoint p) {
        logger.trace("ENTRY:");
        logger.debug("insert0: InsertPoint={}", p);
        expectDelayedResponse = false;
        try {
            // XXX: この部分は論文で抜けている (k-abe)
            // XXX: Locatorの値が変化する場合，equalsだとまずいだろう．
            if (p.left.equals(me)) {
                logger.warn("it seems that i'm unknowningly inserted!");
                askRemoteNodeToRemoveMe(p.right);
                return false;
            }
            protoLock.writeLock().lock();
            try {
                setLeft(p.left);
                // because the repair-count in the left link number may be
                // incremented after (previous) unsuccessful insertion,
                // we do not call setLeftNum(new LinkNum(0, 0)) here.
                setRight(p.right);
                setRightNum(null);
                setMode(Mode.INS);
            } finally {
                protoLock.writeLock().unlock();
            }
            logger.debug("insert0: {} inserts between {}", key, p);
            // send SetR message to the left node
            int reqNo = futures.newFuture();
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, reqNo, me, right, lNum, 1, null);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                logger.error("", e.getCause());
                return false;
            }

            // wait for setRAck or setRNak
            try {
                futures.get(reqNo, SETR_TIMEOUT);
            } catch (TimeoutException e) {
                logger.info("{}: insertion timed-out", key);
            }

            protoLock.writeLock().lock();
            switch (mode) {
            case IN:
                // we have received SetRAck
                protoLock.writeLock().unlock();
                return true;
            case OUT:
                // we have received SetRNak
            case INS:
                // time-out waiting for SetRAck or SetRNak.
                // because the left node may have received the SetR message
                // we sent, and currently this node has insufficient information
                // (e.g., no neighbors, no rNum), try to remove this node.
                
                // before asking the right node to fix, set mode to OUT
                // because getLiveLeftNodeStat() returns a node whose mode
                // is INS.
                setMode(Mode.OUT);
                // also, increment the repair-count of the left link number. 
                setLeftNum(lNum.gnext());
                protoLock.writeLock().unlock();
                askRemoteNodeToRemoveMe(right);
                break;
            default:
                protoLock.writeLock().unlock();
                // empty
            }
            return false;
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * ask a remote node to remove myself (me). 
     * @param remote    the remote node to ask.
     */
    private void askRemoteNodeToRemoveMe(Link remote) {
        logger.debug("ask {} to remove me", remote);
        try {
            NodeManagerIf stub = getStub(remote.addr);
            stub.startFix(remote.key, me, true);
        } catch (RPCException e) {
            logger.error("", e.getCause());
        }
    }

    /**
     * delete this node from the linked-list.
     * if the DDLL protocol fails to delete this node after 
     * maxRetry retries, the node is forcibly deleted and returns false.
     * 
     * @param maxRetry max retry
     * @return true if successfully deleted
     * @throws IllegalStateException thrown if node is not inserted
     */
    public boolean delete(int maxRetry) throws IllegalStateException {
        if (mode != Mode.IN) {
            throw new IllegalStateException("already deleted");
        }
        boolean rc = false;
        try {
            for (int i = 0; i < maxRetry; i++) {
                if (i != 0) {
                    sleepForRetry(i);
                }
                logger.debug("trying to delete {}, i={}", this, i);
                if (rc = delete0()) {
                    break;
                    //manager.unregisterNode(key);
                    //manager.monitor.unregisterNode(left, this);
                    //return true;
                }
            }
        } catch (OfflineSendException e) {
            logger.info("delete failed as offline");
        } finally {
            reset();
        }
        return rc;
    }

    /**
     * delete this node from the linked-list.
     * 
     * @return true if successfully deleted
     * @throws OfflineSendException thrown if this node is offline
     */
    protected boolean delete0() throws OfflineSendException {
        logger.trace("ENTRY:");
        protoLock.writeLock().lock();
        try {
            // XXX: 論文のA8, A9では右ノードのみが等しいかをチェックしているが，
            // 左ノードもチェックする．
            // ノードuの左ノードが別のノードaを指しているとき，aはuにSetLを
            // 送ってくる可能性がある．このため，右ノードだけチェックして早々に
            // OUTにしてしまうと，SetLに対応できない．(k-abe)
            if (me.equals(right) && me.equals(left)) {
                reset();
                return true;
            }
            if (mode == Mode.DEL) {
                // retry because timed out
                // InsertPoint p = findLiveLeft();
                // if (p == null) {
                // logger.info("findLiveLeft could not find node");
                // return false;
                // }
                // protoLock.writeLock().lock();
                // left = p.left;
                // lNum = lNum.gnext();
                // protoLock.writeLock().unlock();
            } else {
                setMode(Mode.DEL);
            }

            // send SetR message to the left node
            int reqNo = futures.newFuture();
            delReqNo = reqNo;
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, reqNo, right, me, rNum.next(), 1, null);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return false;
            }

            // wait for an unrefL or the a setRNak message.
            // if we receive a unrefL message, mode becomes OUT.
            // if we receive a setRNak message, mode becomes DELWAIT.
            // (see Node.unrefL() and Node.setRNak() methods)
            while (mode != Mode.OUT && mode != Mode.DELWAIT) {
                try {
                    logger.debug("{} delete condwaiting", key);
                    protoCond.await(DELETE_OP_TIMEOUT, TimeUnit.MILLISECONDS);
                    logger.debug("{} delete condwait done {}", key, mode);
                } catch (InterruptedException e1) {
                    logger.debug("{} delete condwait interrupted {}", key, mode);
                }
                if (mode == Mode.DEL) {
                    // timeout waiting for SetRAck message from the left node
                    /*
                     * SetRAckがタイムアウトしたら，右ノードにSetLを送ってしまう． リンクの修復は右ノードに任せる．
                     * (mode = GRACEで右ノードからのUnrefLを待つ．UnrefLが来たら mode = OUT になる)
                     */
                    logger.debug("{} left node timed-out while mode=DEL", key);
                    setMode(Mode.GRACE);
                    lastUnrefL = false;
                    try {
                        sendSetL(right, left, rNum.next(), me, false);
                    } catch (RPCException e2) {
                        if (e2 instanceof OfflineSendException) {
                            throw (OfflineSendException) e2;
                        } else {
                            logger.error("", e2.getCause());
                        }
                    }
                    continue;
                } else if (mode == Mode.GRACE) {
                    // timeout waiting for UnrefL message from the right node
                    setMode(Mode.OUT);
                    // unrefLを左へ送る
                    logger.debug("{} send unrefL to {}", key, left);
                    try {
                        NodeManagerIf stub = getStub(left.addr);
                        stub.unrefL(left.key, me);
                    } catch (RPCException e) {
                        if (e instanceof OfflineSendException) {
                            throw (OfflineSendException) e;
                        } else {
                            logger.error("", e.getCause());
                        }
                    }
                    //setLeft(null);    (dup with reset()) 
                    //setRight(null);
                    break;
                }
            }
            if (mode == Mode.DELWAIT) {
                return false;
            }
            return true; // mode == OUT
        } finally {
            protoLock.writeLock().unlock();
            logger.trace("EXIT:");
        }
    }

    /**
     * insert/deleteのリトライの際の待ち時間を計算し、その間sleepする。
     * <p>
     * n 回目のリトライの際には、 [0, n * INS_DEL_RETRY_INTERVAL_BASE] の範囲の乱数値を待ち時間に使う。
     * 
     * @param n リトライ回数
     */
    private void sleepForRetry(int n) {
        protoLock.writeLock().lock();
        try {
            long t =
                    (long) ((n + Math.random() * 0.1) * INS_DEL_RETRY_INTERVAL_BASE);
            logger.debug("{}th retry after {}ms", n, t);
            protoCond.await(t, TimeUnit.MILLISECONDS); // XXX: Why?
        } catch (InterruptedException ignore) {
        }
        protoLock.writeLock().unlock();
    }

    /**
     * find the immediate left and right nodes of `searchKey', by contacting the
     * specified introducer node.
     * 
     * @param introducer introducer node
     * @param searchKey
     * @param maxHops max hops
     * @return insert point. null on errors.
     * @throws OfflineSendException
     */
    private InsertPoint findInsertPoint(Link introducer, DdllKey searchKey,
            int maxHops) throws OfflineSendException {
        // if (GOD_MODE) {
        // return NodeArray4Test.findInsertPoint(searchKey);
        // }

        Link next = introducer;
        Link prevKey = null;
        for (int i = 0; i < maxHops; i++) {
            logger.debug("findInsertPoint: [{}]: i = {}, next = {}", key, i,
                    next);
            int reqNo = futures.newFuture();
            try {
                NodeManagerIf stub = getStub(next.addr);
                stub.findNearest(next.key, me, reqNo, searchKey, prevKey);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return null;
            }

            try {
                Object result = futures.get(reqNo, SEARCH_OP_TIMEOUT);
                if (result instanceof InsertPoint) {
                    InsertPoint rc = (InsertPoint) result;
                    if (rc.left == null) {
                        // remote node is OUT
                        logger.debug("findInsertPoint: remote node is out: {}",
                                next);
                        return null;
                    }
                    return rc;
                } else if (result instanceof Object[]) {
                    next = (Link) ((Object[]) result)[0];
                    prevKey = (Link) ((Object[]) result)[1];
                } else {
                    logger.error("illegal result");
                    return null;
                }
            } catch (TimeoutException e) {
                logger.warn("{}: no response from {} for findNearest request",
                        this, next);
                return null;
            }
        }
        logger.warn("over max hops");
        return null;
    }

    /*
     * protocol message handlers
     */
    /**
     * SetR message handler.
     * 
     * <pre>
     * pseudo code:
     * (A2)
     * receive SetR(rnew , rcur , rnewnum , incr) from q →
     *   if ((s  ̸= in ∧ s  ̸= lwait) ∨ r  ̸= rcur) then send SetRNak() to q
     * else send SetRAck(rnum) to q; r, rnum, ref := rnew, rnewnum, ref + incr fi
     * 
     * <pre>
     * 
     * @param sender    the sender of this SetR message
     * @param reqNo     request number
     * @param rNew      the new right link number
     * @param rCur      the current right link that this node should have
     * @param rNewNum   the new right link
     * @param incr      the increment to be added to {@link ref} 
     * @param payload   the optional data transferred along with the SetR
     * message
     */
    public void setR(Link sender, int reqNo, Link rNew, Link rCur,
            LinkNum rNewNum, int incr, Object payload) {
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("{} rcvs SetR(rNew={}, rCur={}, rNewNum={}, payload={})",
                this, rNew, rCur, rNewNum, payload);
        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in setR");
                // 応答を返さない
                return;
            }
            if ((mode != Mode.IN && mode != Mode.DELWAIT)
                    || !right.equals(rCur)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setRNak(sender.key, me, reqNo);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setRNak failed "
                                + "while offline in setR()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                } catch (Throwable e) {
                    logger.error("", e);
                }
            } else {
                LinkNum oldNum = rNum;
                setRight(rNew);
                setRightNum(rNewNum);
                setRef(ref + incr);

                if (oldNum.repair < rNum.repair) {
                    // 修復の場合，右ノードは故障しているはず
                    leftNbrs.removeNode(rCur);
                    leftNbrs.add(rNew);
                } else {
                    boolean forInsertion = sender.equals(rNew);
                    if (forInsertion) {
                        leftNbrs.add(rNew);
                    } else {
                        leftNbrs.removeNode(rCur);
                    }
                }
                Set<Link> nset = leftNbrs.computeSetForRNode(true, rNew);
                leftNbrs.setPrevRightSet(rNew, nset);
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setRAck(sender.key, me, reqNo, oldNum, nset);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setRAak failed "
                                + "while offline in setR()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                } catch (Throwable e) {
                    logger.error("", e);
                }
                // if this SetR is for node deletion or recovery,
                // send my left neighbors to the new right node
                /*if (!sender.equals(rNew) || incr == 0) {
                    leftNbrs.sendRight(right, key);
                }*/
                if (observer != null) {
                    protoLock.writeLock().unlock();
                    // notify the application
                    observer.onRightNodeChange(rCur, rNew, payload);
                }
            }
        } finally {
            logger.trace("EXIT:");
            if (protoLock.isWriteLockedByCurrentThread()) {
                protoLock.writeLock().unlock();
            }
        }
    }

    /**
     * SetRAck message handler.
     * 
     * <pre>
     * pseudo code:
     * (A3) receive SetRAck(znum) from q →
     *   if (s = jng) then s, rnum, ref := in, znum.next(), 1; send SetL(u, rnum, l) to r
     *     elseif (s = lvg) then s := grace; send SetL(l, rnum.next(), u) to r
     *     elseif (s = fix) then s := in fi
     * </pre>
     * 
     * @param sender the sender of this SetRAck message
     * @param reqNo request number
     * @param zNum the previous right link number of the sender node
     * @param nbrs neighbor node set
     */
    public void setRAck(Link sender, int reqNo, LinkNum zNum, Set<Link> nbrs) {
        /*
         */
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("{} rcvs SetRAck(sender={}, reqNo={}, zNum={})", this,
                sender, reqNo, zNum);

        try {
            if (!sender.equals(left)) {
                logger.info(
                        "left node is changed before receiving setRAck, mode {}",
                        mode);
            }
            if (reqNo != 0 && futures.expired(reqNo)) {
                logger.info("setR is expired in setRAck");
                return;
            }
            if (fixState == FIX_STATE.FIXING) {
                logger.info("{}: setRAck received from {} while fixing==true",
                        this, sender);
                notFixing();
                leftNbrs.set(nbrs);
                leftNbrs.sendRight(right, sender.key);
                return;
            }
            switch (mode) {
            case INS:
                // we are in the middle of inserting a node
                setMode(Mode.IN);
                setRightNum(zNum.next());
                setRef(1);
                leftNbrs.set(nbrs);
                // insert callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo);
                try {
                    sendSetL(right, me, rNum, sender, true);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setL failed "
                                + "while offline in setRAck()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
                // let the right node know that new node (me) has been
                // inserted.
                // TODO: この処理はSetLメッセージにpiggy-backできそう．
                // leftNbrs.sendRight(right);
                break;
            case DEL:
                // we are in the middle of deleting a node
                setMode(Mode.GRACE);
                // delete callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo);
                // send SetL message to the right node
                try {
                    sendSetL(right, left, rNum.next(), me, false);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setRNak failed "
                                + "while offline in setR()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
                break;
            default:
                if (expectDelayedResponse) {
                    logger.debug("maybe received a delayed SetRAck");
                } else {
                    logger.warn("mode={} when SetRAck is received", mode);
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    /**
     * SetRNak message handler.
     * 
     * <pre>
     * pseudo code:
     * (A4)
     * receive SetRNak() from q →
     *   if (s = jng) then s := jwait
     *   elseif (s = lvg) then s := lwait
     *   elseif (fixing) then fixing := false fi
     * </pre>
     * 
     * @param sender the sender of this SetRNak message
     * @param reqNo request number
     */
    public void setRNak(Link sender, int reqNo) {
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("{} rcvs SetRNak(sender={}, reqNo={})", sender, reqNo);
        try {
            if (reqNo != 0 && futures.expired(reqNo)) {
                logger.info("setR is expired in setRNak");
                return;
            }
            if (fixState == FIX_STATE.FIXING) {
                logger.info("{}: setRNak received from {} while FIXING", this,
                        sender);
                notFixing();
                return;
            }
            switch (mode) {
            case INS:
                if (!sender.equals(left)) {
                    logger.warn("received SetRNak from unexpected sender {}, ignored",
                            sender);
                    break;
                }
                setMode(Mode.INSWAIT);
                // insert callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo);
                break;
            case DEL:
                setMode(Mode.DELWAIT);
                // delete callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo);
                protoCond.signalAll();
                break;
            default:
                if (expectDelayedResponse) {
                    logger.debug("maybe received a delayed SetRNak");
                } else {
                    logger.warn("illegal mode: {} in setRNak", mode);
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void setL(Link lNew, LinkNum lNewNum, Link prevL, Set<Link> nbrs) {
        /*
         * (A5) receive SetL(lnew , lnewnum , d) from q → if (lnewnum>lnum) then
         * l, lnum := lnew, lnewnum fi send UnrefL() to d
         */
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("{} rcvs SetL(lNew={}, lNewNum={}, prevL={})", this, lNew,
                lNewNum, prevL);

        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in setL");
                // 応答を返さない
                return;
            }
            boolean leftChanged = false;
            if (lNum.compareTo(lNewNum) < 0) {
                setLeft(lNew);
                setLeftNum(lNewNum);
                leftChanged = true;
            } else {
                logger.info("SetL ignored (lNum={}, lNewNum={})", lNum, lNewNum);
            }
            try {
                NodeManagerIf stub = getStub(prevL.addr);
                stub.unrefL(prevL.key, me);
            } catch (RPCException e) {
                if (e instanceof OfflineSendException) {
                    logger.warn("{} :sending unrefL failed "
                            + "while offline in setL()", this);
                } else {
                    logger.error("", e.getCause());
                }
            }

            /*
             * 近隣ノード集合を書き換えて，右ノードに伝達．
             */
            leftNbrs.set(nbrs);
            // 1 2 [3] 4
            // 自ノードが4とする．
            // [3]を挿入する場合，4がSetL受信．この場合のlimitは2 (lPrev=2, lNew=3)
            // [3]を削除する場合，4がSetL受信．この場合のlimitも2 (lPrev=3, lNew=2)
            if (Node.isOrdered(prevL.key, lNew.key, key)) {
                // this SetL is sent for inserting a node (lNew)
                leftNbrs.sendRight(right, prevL.key);
            } else {
                // this SetL is sent for deleting a node (prevL)
                leftNbrs.sendRight(right, lNew.key);
            }

            /*
             * Some optimization (論文には書いていない) k-abe
             */
            if (leftChanged) {
                if (mode == Mode.INSWAIT || mode == Mode.DELWAIT) {
                    logger.debug("wakeup insert or delete thread");
                    protoCond.signalAll();
                }
            }
            if (leftChanged && mode == Mode.DEL) {
                if (me.equals(right) && me.equals(left)) {
                    // 自ノードが削除のためにSetRを送信中に，SetLを受信し，それによって
                    // 自ノードの左ポインタが自ノードを指している．
                    // また，右ノードも自ノードを指しているので，自ノードは唯一のノード
                    // である．このため，自ノードはSetRAckを待たずにOUT状態に遷移する．
                    // このとき，SetRAck/Nakが後から到着する可能性があることを示すため，
                    // expectDelayedResponseをセットする．
                    logger.info("{}: SetL received while mode=DEL, case l=u=r",
                            this);
                    setMode(Mode.OUT);
                    setLeft(null);
                    setRight(null);
                    // proceed the thread blocked in delete0()
                    futures.set(delReqNo);
                    expectDelayedResponse = true;
                    protoCond.signalAll();
                } else if (me.equals(left)) {
                    // XXX: この状況になることになる条件が不明!!!
                    logger.info("{}: SetL received while mode=DEL, case l=u",
                            this);
                    setMode(Mode.GRACE);
                    futures.set(delReqNo);
                    expectDelayedResponse = true;
                    // proceed the thread blocked in delete0()
                    protoCond.signalAll();
                    try {
                        sendSetL(right, left, rNum.next(), me, true);
                    } catch (RPCException e) {
                        if (e instanceof OfflineSendException) {
                            logger.warn("{}: sending setRNak failed "
                                    + "while offline in setR()", this);
                        } else {
                            logger.error("", e.getCause());
                        }
                    }
                } else {
                    logger.info("{}: SetL received while mode=DEL, case l!=u",
                            this);
                    /*
                     * XXX: 新しい左ノードにSetRを再送したいが，delete0側の
                     * 同期処理を変更する必要があるのでとりあえずこのままとする．
                     * (delete0は自分で送信したSetRに対応するreqNoに対する応答だけ を待ち合わせるのが問題)
                     * このままでもdelete0で送ったSetRがタイムアウトして 削除処理は終了する．(k-abe)
                     */
                    /*
                     * int reqNo = futures.newFuture(); try { NodeManagerIf stub
                     * = getStub(left.addr); stub.setR(left.key, me, reqNo,
                     * right, me, rNum.next(), 1); } catch
                     * (RPCException e) {
                     * futures.discardFuture(reqNo); if (e.getCause() instanceof
                     * OfflineSendException) { logger.warn(this +
                     * " :sending setR failed " + "as offline in setL"); } else
                     * { logger.error("", e.getCause()); } }
                     */
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void unrefL(Link sender) {
        /*
         * (A6) receive UnrefL() from q → ref := ref − 1 if (ref = 0) then send
         * UnrefL() to l; s := out fi
         */
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("{} rcvs UnrefL(sender={})", this, sender);
        try {
            setRef(ref - 1);
            if (ref == 0) {
                if (mode != Mode.GRACE) {
                    logger.warn("mode is not GRACE in unrefL: ({}->{})"
                            + sender, this);
                    // 応答を返さない
                    return;
                }
                if (lastUnrefL) {
                    logger.debug("{} SEND2 UNREFL TO {}", key, left);
                    try {
                        NodeManagerIf stub = getStub(left.addr);
                        stub.unrefL(left.key, me);
                    } catch (RPCException e) {
                        if (e instanceof OfflineSendException) {
                            logger.warn("{}: sending unrefL failed "
                                    + "while offline in unrefL()", this);
                        } else {
                            logger.error("", e.getCause());
                        }
                    }
                }
                setMode(Mode.OUT);
                setLeft(null);
                setRight(null);
                protoCond.signalAll();
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void setFindResult(int reqNo, Link left, Link right) {
        protoLock.readLock().lock();
        try {
            // insert callのawaitをはずす
            futures.set(reqNo, new InsertPoint(left, right));
        } finally {
            protoLock.readLock().unlock();
        }
    }

    public void setFindNext(int reqNo, Link next, Link prevKey) {
        protoLock.readLock().lock();
        try {
            // insert callのawaitをはずす
            futures.set(reqNo, new Object[] { next, prevKey });
        } finally {
            protoLock.readLock().unlock();
        }
    }

    // on hop nodes
    public void findNearest(Link sender, int reqNo, DdllKey searchKey,
            Link prevKey) {
        protoLock.readLock().lock();
        logger.trace("ENTRY:");
        logger.debug(
                "{} rcvs findNearest(sender={}, reqNo={}, searchKey={}, prevKey={})",
                this, sender, reqNo, searchKey, prevKey);

        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in findNearest ({}->{})", sender, me);
                // XXX: ここで応答を返さないと，Skip graphの挿入時にタイムアウト待ちに
                // なるノードがチェーン状に発生する可能性があるため，応答を返すことにする．
                // (k-abe)
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindResult(sender.key, reqNo, null, null);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setFindResult failed "
                                + "while offline in findNearest()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
                return;
            }
            if (prevKey == null) {
                prevKey = keyComp.compare(key, searchKey) < 0 ? left : right;
            }
            if (mode != Mode.GRACE && isOrdered(key, searchKey, right.key)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindResult(sender.key, reqNo, me, right);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setFindResult failed "
                                + "while offline in findNearest()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
            } else if (mode != Mode.GRACE
                    && isOrdered(key, searchKey, prevKey.key)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindNext(sender.key, reqNo, right, prevKey);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setFindNext failed "
                                + "while offline in findNearest()", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
            } else {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindNext(sender.key, reqNo, left, me);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        logger.warn("{}: sending setFindNext failed "
                                + "while offline in findNearest", this);
                    } else {
                        logger.error("", e.getCause());
                    }
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.readLock().unlock();
        }
    }

    public void getStat(Link sender, int reqNo) {
        protoLock.readLock().lock();
        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in getStat ({}->{})", sender, me);
                // 応答を返さない
                return;
            }
            // for debugging
            if (mode == Mode.IN && (left == null || right == null)) {
                logger.warn("getStat: something wrong: {}", me);
            }
            try {
                NodeManagerIf stub = getStub(sender.addr);
                stub.setStat(sender.key, reqNo, new Stat(mode, me, left, right,
                        rNum));
            } catch (RPCException e) {
                if (e instanceof OfflineSendException) {
                    logger.warn("{}: sending setStat failed "
                            + "while offline in getStat()", this);
                } else {
                    logger.error("", e.getCause());
                }
            }
        } finally {
            protoLock.readLock().unlock();
        }
    }

    /**
     * returns the status of this node.
     * <p>
     * this method is called by
     * {@link NodeManager#getStatMulti(Endpoint, DdllKey[])} to gather status of
     * multiple Node instances.
     * 
     * @return status of this node
     */
    Stat getStatNew() {
        protoLock.readLock().lock();
        // logger.debug("getStatMulti is called at {}", me);
        try {
            return new Stat(mode, me, left, right, rNum);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void setStat(int reqNo, Stat stat) {
        protoLock.readLock().lock();
        try {
            // getStat callのawaitをはずす
            futures.set(reqNo, stat);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void propagateNeighbors(Set<Link> newset, DdllKey limit) {
        protoLock.readLock().lock();
        logger.trace("ENTRY:");
        logger.trace("{} rcvs propagateNeighbors(newset={})", this, newset);
        try {
            leftNbrs.receiveNeighbors(newset, right, limit);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void sendSetL(Link dest, Link lNew, LinkNum lNewNum, Link lPrev,
            boolean isInserted) throws RPCException {
        // compute the neighbor node set at the dest node.
        Set<Link> nset = leftNbrs.computeSetForRNode(isInserted, dest);
        // sending a SetL message is equivalent to sending a propagateNeighbors
        // message.  make leftNbrs know this fact. 
        leftNbrs.setPrevRightSet(dest, nset);
        NodeManagerIf stub = getStub(dest.addr);
        stub.setL(dest.key, lNew, lNewNum, lPrev, nset);
    }

    /*
     * link repairing (link fixing) related procedures
     */
    /**
     * called from {@link NodeMonitor.NodeMon#ping()} when a node failure is
     * detected.
     * 
     * @param failedLink
     */
    void onNodeFailure(final Collection<Link> failedLinks) {
        if (observer != null) {
            boolean rc = observer.onNodeFailure(failedLinks);
            if (rc) {
                startFix(failedLinks);
            }
        } else {
            startFix(failedLinks);
        }
    }

    /**
     * start fixing a single failed link.
     * 
     * @param failed
     * @param force     true if you want to check the left node even if it is
     *                  not equals to `failed'.
     */
    public void startFix(Link failed, boolean force) {
        startfix(Collections.singleton(failed), null, force);
    }

    public void startFix(Link failed) {
        startFix(failed, false);
    }

    /**
     * start fixing set of failed links.
     * 
     * @param failedLinks
     */
    public void startFix(final Collection<Link> failedLinks) {
        startfix(failedLinks, null, false);
    }

    public void startfix(final Collection<Link> failedLinks, Object payload) {
        startfix(failedLinks, payload, false);
    }

    /**
     * start fixing `failedLinks'. `payload' is piggy-backed by SetR message if
     * it is non-null.
     * 
     * @param failedLinks
     * @param payload
     * @param force     true if you want to check the left node even if it is
     *                  not contained in `failedLinks'.
     */
    public void startfix(final Collection<Link> failedLinks,
            final Object payload, boolean force) {
        Link left = getLeft();
        if (left == null || (!force && !failedLinks.contains(left))) {
            logger.debug("startfix: no fix: {}", failedLinks);
            if (observer != null && payload != null) {
                observer.payloadNotSent(payload);
            }
            return;
        }
        leftNbrs.removeNodes(failedLinks);
        Thread th = new Thread(THREAD_NAME_PREFIX + thNum.getAndIncrement()) {
            @Override
            public void run() {
                try {
                    logger.debug("start fixing (failedKeys = {})", failedLinks);
                    fix(failedLinks, payload);
                } catch (OfflineSendException e) {
                }
            }
        };
        int num = fixCount.getAndIncrement();
        th.setName("FIX" + key + "(failedKeys=" + failedLinks + ")#" + num);
        th.start();
        return;
    }

    private AtomicInteger fixCount = new AtomicInteger(0);

    /**
     * called when NodeMonitor receives a Stat message from the left node.
     * 
     * @param s Stat message just received
     */
    void statReceived(Stat s) {
        protoLock.readLock().lock();
        if ((mode == Mode.IN || mode == Mode.DELWAIT) && getLeft().equals(s.me)) {
            if (me.equals(s.right)) {
                logger.trace("ENTRY:");
            } else {
                logger.debug("statReceived: mismatch with left node: {}", s);
                Thread th =
                        new Thread(THREAD_NAME_PREFIX + thNum.getAndIncrement()) {
                            @Override
                            public void run() {
                                try {
                                    // TODO: fix()は，再度 getStat を要求してしまう．
                                    // sを使うように改善するべき．
                                    fix(null, null);
                                } catch (OfflineSendException e) {
                                }
                            }
                        };
                th.setName("FIX" + key + "(mismatch)");
                th.start();
            }
        } else {
            logger.debug("statReceived: ignore: {}", s);
        }
        protoLock.readLock().unlock();
    }

    /*
     * State Transition Diagram:
     *
     *          fix()          left node NG
     * WAITING --------> CHECKING --------> FIXING
     *    |  left node OK   |                 |
     *    ^-----------------+                 |
     *    ^-----------------------------------'
     *   (SETR_OP_TIMEOUT, SetRAck, SetRNak, SetL)
     *
     * WAITING : 待機中．
     * CHECKING: fix()で左ノードのチェック中．
     * FIXING  : fix()で左ノードの故障を検出し，SetRメッセージを送信した状態．
     *
     * メモ:
     * ・状態遷移の排他制御には，writeLock を使う．
     * ・WAITINGへの遷移は notFixing() で行う．
     * ・変数 fixTask は，FIXING状態でSetRがタイムアウトしたかをチェックするTimerTaskを
     *   保持．
     * ・SetRAck, SetRNakで WAITING に遷移するときは，FixTask をキャンセル．
     * ・FIXINGの間にSetLメッセージを受信して左ノードが書き換わった場合，WAITING状態へ遷移．
     */

    private volatile long lastFix = 0;
    /**
     * check the left node and recover the left link if necessary.
     * <p>
     * the calling thread returns without confirming the recovery of the left
     * link. however, if the recovery attempt fails, another thread is created
     * and keeps trying to recover the left link so eventually the left link is
     * recovered.
     * 
     * @param failed known failed links
     * @param payload an Object to be sent with SetR message. null is ok.
     * @throws OfflineSendException
     */
    private void fix(final Collection<Link> failed, Object payload)
            throws OfflineSendException {
        boolean callObserver = false;
        protoLock.writeLock().lock();
        try {
            if (fixState != FIX_STATE.WAITING) {
                logger.debug("fix(): another thread is fixing: {}", fixState);
                if (observer != null && payload != null) {
                    // wait until the current fix procedure executed by
                    // another thread terminates and then
                    // call the observer#payloadNotSent() to retransmit the
                    // payload.
                    while (fixState != FIX_STATE.WAITING) {
                        logger.debug("fix(): waiting (fixState = {})", fixState);
                        try {
                            protoCond.await();
                        } catch (InterruptedException e) {
                        }
                        logger.debug("fix(): waiting done");
                    }
                    // call observer after unlocking
                    callObserver = true;
                }
                return;
            }
            /*
             * {Fix} [] (A11) (s = in ∨ s = delwait ∨ s = grace) ∧ ¬fixing →
             * {execute periodically} (v, vr, vrnum) := getLiveLeftStat() if (s
             * = grace) then if (l ̸= v) then l, lastUnrefL := v, false fi skip
             * fi if (v = l ∧ vr = u ∧ vrnum = lnum) then skip fi {left link is
             * consistent} l , lnum , fixing := v, lnum.gnext(), true; send
             * SetR(u, vr , lnum , 0) to l [] (A12) timeout fixing → fixing :=
             * false
             */

            fixState = FIX_STATE.CHECKING;
            if (mode != Mode.IN && mode != Mode.DELWAIT && mode != Mode.GRACE) {
                return;
            }
            // -- unlock region
            protoLock.writeLock().unlock();
            // To avoid rush of fixing, make minimum interval between fixing.
            if (System.currentTimeMillis() - lastFix < MIN_FIX_INTERVAL) {
                try {
                    Thread.sleep(MIN_FIX_INTERVAL);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
		
            Stat v;
            try {
                v = getLiveLeftStat(failed);
                logger.debug("{}: fix(): getLiveLeftStat returns: {}", me, v);
                if (v == null) {
                    if (mode == Mode.IN || mode == Mode.DELWAIT) {
                        logger.warn(
                                "{}: getLiveLeftStat could not find any live node: {}",
                                me, leftNbrs);
                    }
                    return;
                }
            } finally {
                protoLock.writeLock().lock();
            }
            // lockを外している間に状態が変更されている可能性があるので再度チェック
            if (mode != Mode.IN && mode != Mode.DELWAIT && mode != Mode.GRACE) {
                return;
            }
            // --
            if (mode == Mode.GRACE) {
                if (!left.equals(v.me)) {
                    setLeft(v.me);
                    lastUnrefL = false;
                    // XXX: callObserver?
                }
                return;
            }
            // note that v.rNum is null if v.s = INS
            if (v.me.equals(left) && v.right.equals(me) && lNum.equals(v.rNum)) {
                // left link is consistent
                if (payload != null) {
                    callObserver = true;
                    logger.debug("{}: SetR not send payload", me);
                }
                return;
            }
            setLeft(v.me);
            setLeftNum(lNum.gnext());
            logger.info("{}: (A11) fix.  send SetR to {}", me, left);
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, 0, me, v.right, lNum, 0, payload);
            } catch (RPCException e) {
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
            }
            /*
             * 修復のために代替左ノードを発見して SetR メッセージを送信したので， そのノードからの SetRAck/SetRNak
             * メッセージを待つ．
             */
            fixState = FIX_STATE.FIXING;
            fixTask = new TimerTask() {
                // timer is expired (means the previous attempt to fix the left
                // link failed)
                @Override
                public void run() {
                    boolean retry = false;
                    protoLock.writeLock().lock();
                    if (fixState == FIX_STATE.FIXING) {
                        // SetRAckもSetRNakも受信していないので，再修復を試みる．
                        logger.debug("{}: no SetRAck/SetRNak is received "
                                + "within SETR_TIMEOUT", me);
                        fixTask = null;
                        retry = true;
                        notFixing();
                    }
                    protoLock.writeLock().unlock();
                    if (retry) {
                        // add the failed link to `failed'
                        Collection<Link> f = new HashSet<Link>();
                        if (failed != null) {
                            f.addAll(failed);
                        }
                        f.add(left);
                        startFix(f);
                    }
                }
            };
            stabilizeTimer.schedule(fixTask, Node.SETR_TIMEOUT);
        } finally {
            if (fixState == FIX_STATE.CHECKING) {
                // fixStateがCHECKINGのままということは，修復する必要がなかったということ．
                notFixing();
                assert fixState == FIX_STATE.WAITING;
            }
            lastFix = System.currentTimeMillis();
            protoLock.writeLock().unlock();
            if (callObserver && observer != null && payload != null) {
                observer.payloadNotSent(payload);
            }
        }
    }

    /**
     * FIXING (あるいは CHECKING) からWAITINGに移行する．
     */
    private void notFixing() {
        logger.debug("notFixing: {}", this);
        assert protoLock.isWriteLockedByCurrentThread();
        fixState = FIX_STATE.WAITING;
        protoCond.signalAll(); // wake up threads blocked in fix() if any
    }

    /**
     * search the live immediate left node.
     * 
     * @param failedLinks known failed links. the nodes listed in failedLinks
     *            are considered being failed without checking.
     * @return the status of the immediate live left node.
     * @throws OfflineSendException
     */
    private Stat getLiveLeftStat(Collection<Link> failedLinks)
            throws OfflineSendException {
        if (GOD_MODE) {
            return NodeArray4Test.getLiveLeftStat(this);
        }
        Stat s = getLiveLeftStat1(failedLinks);
        if (!COMPARE_WITH_GOD) {
            return s;
        }
        Stat god = NodeArray4Test.getLiveLeftStat(this);
        if (god == null || s == null
                || (god != null && s != null && !god.me.equals(s.me))) {
            logger.warn("getLiveLeftStat: god  = {}", god);
            logger.warn("getLiveLeftStat: ours = {}", s);
            logger.warn("nbrs = {}", leftNbrs);
        }
        if (s != null && (s.mode == Mode.INSWAIT || s.mode == Mode.OUT)) {
            logger.error("how strange! {}", s);
            System.exit(1);
        }
        return s;
    }

    /**
     * search the live immediate left node by using leftNbrs.
     * 
     * @param failedLinks known failed links. the nodes listed in failedLinks
     *            are considered being failed without checking.
     * @return the status of the immediate live left node.
     * @throws OfflineSendException
     */
    private Stat getLiveLeftStat1(Collection<Link> failedLinks)
            throws OfflineSendException {
        Map<Link, Stat> stats = new HashMap<Link, Stat>();
        if (failedLinks != null) {
            // register the failed node so that we do not have to traverse
            // rightward from the left node of the failed node
            for (Link failed : failedLinks) {
                stats.put(failed, null);
            }
        }
        NeighborSet cands = new NeighborSet(me, manager, 0);
        cands.addAll(leftNbrs.leftNbrSet);
        // 自ノードの他のキーを追加する．
        // キーのidが同じキーは論理的に同じ連結リスト上にあるという前提
        Set<Link> others = manager.getAllLinksById(key.id);
        others.remove(me);
        if (others.size() > 0) {
            logger.debug("{}: add other keys on the local node {}", me, others);
            cands.addAll(others);
        }
        // add external links not managed by DDLL
        List<Link> ext = observer.suppplyLeftCandidatesForFix();
        if (ext != null) {
            cands.addAll(ext);
        }
        logger.info("{}: candidates={}", me, cands);
        List<Link> nbrs = new ArrayList<Link>(cands.leftNbrSet);

        // find the closest node in the left side whose state is IN or DELWAIT
        Link n = null;
        for (Link node : nbrs) {
            // ignore the ghost of myself (XXX: THINK!)
            if (node.key.primaryKey.equals(me.key.primaryKey)
                    && node.addr.equals(me.addr) && !node.equals(me)) {
                continue;
            }
            // logger.debug("getLiveLeftStat: checking {}", node);
            Stat s = getStat(node);
            stats.put(node, s);
            if (s == null) {
                leftNbrs.removeNode(node);
                continue;
            }
            // note that it is okay here if the node is reinserted
            logger.debug("{}: getLiveLeftStat: got stat {}", me, s);
            if (s.mode == Mode.IN || s.mode == Mode.DELWAIT) {
                n = node;
                break;
            }
        }
        if (n == null) {
            // if we do not find any live node, return myself.
            if (nbrs.size() > 0) {
                logger.warn("{}: getLiveLeftStat could not find "
                        + "any IN nor DELWAIT node. (nbrs={})", me, nbrs);
            }
            protoLock.readLock().lock();
            try {
                // check my state (my right node might be null!) 
                if (mode != Mode.IN && mode != Mode.DELWAIT) {
                    logger.debug("{}: getLiveLeftStat1: noticed i've left", me);
                    return null;
                }
                n = me;
                // insert a fake entry
                stats.put(n, new Stat(mode, me, left, right, rNum));
                // return new Stat(mode, me, left, right, rNum);
            } finally {
                protoLock.readLock().unlock();
            }
        }
        // search rightward from node n
        while (true) {
            Stat nstat = stats.get(n);
            Link nr = nstat.right;
            if (nr.key.compareTo(key) == 0) {
                // if n = n.r, return n (mostly n = myself case)
                return nstat;
            }
            if (n.key.compareTo(key) != 0 && isOrdered(n.key, key, nr.key)) {
                // if n < u < n.r, return n
                // logger.info("getLiveLeftStat: loop {}", count);
                return nstat;
            }
            if (stats.containsKey(nr)) {
                if (stats.get(nr) == null) {
                    // nr is failed
                    return stats.get(n);
                }
                // move rightward
                n = nr;
                continue;
            }
            // nr is not listed in leftNbr and (n < nr < u)
            logger.info("{}: getLiveLeftStat found new node {}", me, nr);
            Stat nrstat = getStat(nr);
            if (nrstat == null) {
                // logger.info("getLiveLeftStat: loop {}", count);
                return stats.get(n);
            }
            logger.info("{}: getLiveLeftStat new node stat {}", me, nrstat);
            stats.put(nr, nrstat);
            leftNbrs.add(nr);
            n = nr;
        }
    }

    private Stat getStat(Link node) throws OfflineSendException {
        logger.trace("ENTRY:");
        try {
            int reqNo = futures.newFuture();
            try {
                logger.debug("{}: invoking getStat() on node {}", me, node);
                NodeManagerIf stub = getStub(node.addr);
                stub.getStat(node.key, me, reqNo);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return null;
            }
            try {
                Object result = futures.get(reqNo, GETSTAT_OP_TIMEOUT);
                if (result instanceof Stat) {
                    return (Stat) result;
                } else {
                    logger.error("illegal result");
                    return null;
                }
            } catch (TimeoutException e) {
                logger.warn("getStat to {} is timed out", node);
                return null;
            }
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * a class representing an insertion point in a linked-list.
     */
    public static class InsertPoint implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Link left;
        public final Link right;

        public InsertPoint(Link left, Link right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "[" + left + ", " + right + "]";
        }
    }
}
