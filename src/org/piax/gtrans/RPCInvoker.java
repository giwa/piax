/*
 * RPCInvoker.java - A class for RPC invocation
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RPCInvoker.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.CalleeId;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.impl.RPCInvocationHandler;
import org.piax.util.ClassUtil;
import org.piax.util.MethodUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * ChannelTransportを指定して、そのTransport上でRPCを行うためのクラスである。
 * </p>
 * <p>
 *　RPCは、以下のようにして行う。
 * </p>
 * <pre>
 * int n = rpcInvoker.getStub(AppIf.class,targetObjectId,targetEndpoint).appMethod(arg1,arg2);
 * </pre>
 * <p>
 * ここで、AppIfは、対象のオブジェクトが実装しているインターフェースである。
 * AppIfは、RPCIfを継承していなければいけない。targetObjectIdは、対象のObjectIdである。
 * targetEndpointは、対象のオブジェクトが存在するピアを表すEndpointである。Endpointの実際の型は、
 * このクラスの型パラメータEが示す型でなければいけない。対象は、対象の存在するピアの対応する
 * RPCInvokerに対して、registerRPCObjectを使用して登録されていなければならない。
 * appMethodは、対象が持つメソッドであり、arg1, arg2は、appMethodの引数であるものとする。
 * appMethodの返り値の型はintであるとする。
 * appMethodは、AppIfあるいは、
 * そのスーパー・インターフェースで宣言されていなければいけない。さらに、対象が呼び出し側と異なるピアに存在する場合は
 * {@literal @}RemoteCallableアノテーションがついていて、かつRPCExceptionをスローするように宣言されていなければいけない。
 * 例えば、以下のとうりである。
 * </p>
 * <pre>
 * import org.piax.gtrans.RemoteCallable;
 * 
 * interface AppIf extends RPCIf {
 *   {@literal @}RemoteCallable
 *   int appMethod(int a1, String a2) throws RPCException;
 * }
 * 
 * <h3>引数のコピー</h3>
 * <p>
 * RPCでは、引数は、一般にはコピーされるが、保証されない。呼び出されたメソッド内で引数のオブジェクトを変更した場合、
 * それが呼び出し側に反映されるかどうかは不明であるので注意が必要である。
 * 呼び出された側では、変更しない方が無難である。
 * </p>
 * <h3>oneway RPC</h3>
 * <p>
 * 呼び出したメソッドの終了を待つ通常の同期型RPCの他に、終了を待たないoneway RPCが存在する。
 * oneway RPCでは、呼び出したメソッドの返り値を受け取ることはできない。
 * {@literal @}RemoteCallableアノテーションにType.ONEWAYの引数を指定すると
 * デフォルトではONEWAY RPCとして呼ばれる。
 * oneway RPCを指定しても実際に、非同期になるかどうかは、
 * 使用しているトランスポートに依存するので、注意が必要である。
 * </p>
 *
 * <h3>動的RPC</h3>
 * <p>
 * RPCは原則として対象のインターフェースを指定して行うが
 * 例外的に対象のインターフェースが、実行時まで不明な場合がある。
 * 例えば、デバッグなどで実行時に人間が、メソッドや引数を指定する場合である。
 * この場合は、人間はインターフェースを知っているが、呼び出し側のプログラムには
 * インターフェースが存在しないことがある。
 * このような場合のために、動的な呼び出しを用意している。
 * 以下のように使用する。
 * </p>
 * <pre>
 * try {
 *     Object r = rpcInvoker.rcall(targetObjectId,targetEndpoint,"appMethod",arg1,arg2);
 * } catch (Throwable t) {
 *     // 例外処理
 * }
 * </pre>
 * <p>
 * 返り値は常にObject型となる。intなど基本型はボクシングが行われてInteger型などになる。
 * 例外は、何が発生するか不明なので、Throwableを受けなければいけない。
 * </p>
 * <h3>その他</h3>
 * <p>
 * getStubには、いくつかバリエーションが存在する。
 * timeoutを指定できるもの、RPCModeを指定できるもの、
 * targetObjectId,targetEndpointの代わりにCallerrIdを指定するものが存在する。
 * timeoutが指定できないものでは、GTransConfigValues.rpcTimeoutが使用される。
 * RPCModeを指定できるものでは、指定によりoneway RPCかどうかの決定方法を
 * 以下のように変更可能である。
 * <ul>
 *　<li>AUTOならば、Annotationにより決定する。(デフォルト)</li>
 *  <li>SYNCならば、常に同期型である。</li>
 *  <li>ONEWAYならば、常にOnewayである。</li>
 * </ul>
 * 詳細は、各メソッドの説明を見て欲しい。
 * </p>
 */
public class RPCInvoker<T extends RPCIf, E extends Endpoint> implements RPCIf {
	/*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(RPCInvoker.class);

    public static boolean POOL_CHANNEL = false;
    public Map<Endpoint, Channel<?>> channelPool;
    /**
     * RPCで呼ばれた際の呼び出し側を保存する
     */
    private ThreadLocal<PeerId> srcPeerId = new ThreadLocal<PeerId>();
    
    /**
     * RPCで呼ばれた際の呼び出し側を返す。
     * 同期的local呼び出しでは、変更されないことに注意が
     * 必要である。一度も非同期呼び出しもremote呼び出しも
     * されていない場合はnullを返す。
     * @return 呼び出し側のPeerId
     */
    public PeerId getSrcPeerId() {
        return srcPeerId.get();
    }
    
    public static class MethodCall implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final ObjectId target;
        protected String method;
        protected Object[] args;
        protected PeerId srcPeerId;
        
        protected MethodCall(ObjectId target, PeerId srcPeerId, String method,
                Object... args) {
            this.target = target;
            this.srcPeerId = srcPeerId;
            this.method = method;
            this.args = args;
        }
    }

    protected final TransportId transId;
    protected final ObjectId objId;
    
    protected volatile ChannelTransport<E> trans;
    final Peer peer;
    
    // for call oneway
    final TransportListener<E> listener = new TransportListener<E>() {
        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            _onReceive(rmsg);
        }
    };
    
    // for call sync
    final ChannelListener<E> chListener = new ChannelListener<E>() {
        public boolean onAccepting(Channel<E> ch) {
            return true;
        }
        public void onClosed(Channel<E> ch) {
            // 自動でcloseされるので、ch.closeを呼ぶ必要はない
        }
        public void onFailure(Channel<E> ch, Exception cause) {
        }
        public void onReceive(Channel<E> ch) {
            _onReceive(ch);
        }
    };

    /**
     * RPCInvokerオブジェクトがアクティブな状態であることを示す。
     * fin() が呼ばれた場合はfalseとなる。
     */
    protected volatile boolean isActive = true;

    @SuppressWarnings("unchecked")
    public RPCInvoker(TransportId rpcId, ChannelTransport<? super E> trans) 
            throws IdConflictException, IOException {
        this.transId = rpcId;
        this.objId = createObjId(trans, rpcId);
        this.trans = (ChannelTransport<E>) trans;
        this.trans.setListener(transId, listener);
        this.trans.setChannelListener(transId, chListener);
        peer = Peer.getInstance(trans.getPeerId());
        peer.registerRPCObject(objId, this);
        this.channelPool = new ConcurrentHashMap<Endpoint, Channel<?>>();
        logger.debug("RPCInvoker: rpcId={}, objId={}", rpcId, objId);
    }

    // TODO think!
    // objIdの作成方法
    private ObjectId createObjId(ChannelTransport<? super E> trans,
            ObjectId rpcId) {
        return new ObjectId(trans.getTransportIdPath().toString()
                + ":" + rpcId.toString());
    }

    public void fin() {
        offline();
        isActive = false;
        peer.unregisterRPCObject(objId, this);
        if (POOL_CHANNEL) {
            logger.info("pool size =" + channelPool.size());
            for (Channel<?> ch : channelPool.values()) {
                if (!ch.isClosed()) {
                    ch.close();
                }
            }
            channelPool.clear();
        }
    }

    /**
     * RPCInvokerオブジェクトがアクティブな状態であるかどうかをチェックする。
     * fin() が呼ばれてインアクティブな状態である場合は、IllegalStateExceptionがthrowされる。
     * サブクラスの場合も含め、fin() の後に呼び出されては困る場合のメソッド呼び出しの際のチェックに用いる。
     * 
     * @throws IllegalStateException RPCInvokerオブジェクトがインアクティブな状態である場合
     */
    protected void checkActive() throws IllegalStateException {
        if (!isActive)
            throw new IllegalStateException("this RPCInvoker is already finalized");
    }

    @SuppressWarnings("unchecked")
    public synchronized void changeTransport(ChannelTransport<?> trans) {
        checkActive();
        if (isOnline()) {
            offline();
            this.trans = (ChannelTransport<E>) trans;
            online();
        } else {
            this.trans = (ChannelTransport<E>) trans;
        }
    }
   
    /**
     * RPCの対象となるオブジェクトを登録する。
     * RPCで呼び出されるオブジェクトは予め登録する必要がある。
     * 
     * @param objId 対象のObjectId
     * @param obj 対象オブジェクト
     * @throws IdConflictException
     */
    public void registerRPCObject(ObjectId objId, RPCIf obj)
            throws IdConflictException {
        checkActive();
        peer.registerRPCObject(objId, obj);
    }
    
    /**
     * RPC対象から抹消する。
     * 
     * @param objId　対象のObjectId
     * @param obj 対象オブジェクト
     * @return 削除したらtrue、もともと登録されていない場合はfalseを返す。
     */
    public boolean unregisterRPCObject(ObjectId objId, RPCIf obj) {
        checkActive();
        return peer.unregisterRPCObject(objId, obj);
    }

    public RPCIf getRPCObject(ObjectId objId) {
        return peer.getRPCObject(objId);
    }
    
    @Deprecated
    public synchronized void online() {
        trans.setChannelListener(transId, chListener);
    }

    @Deprecated
    public synchronized void offline() {
        trans.setChannelListener(transId, null);
    }

    @Deprecated
    public synchronized boolean isOnline() {
        return trans.getChannelListener(transId) != null;
    }
    
    public ChannelTransport<E> getTransport() {
        return trans;
    }
    
    public E getEndpoint() {
        return trans.getEndpoint();
    }
    
    /**
     * リモートピア上のこのオブジェクトに対応するRPCInvokerオブジェクトのメソッドを
     * 呼び出すためのstubを返す。
     * 
     * @param remotePeer リモートピアを示すEndpoint
     * @return RPCのためのstub
     */
    public T getStub(E remotePeer) {
        return getStub(remotePeer, GTransConfigValues.rpcTimeout);
    }
    
    /**
     * リモートピア上のこのオブジェクトに対応するRPCInvokerオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    public T getStub(E remotePeer, int timeout) {
        return getStubFor(this.getClass(), objId, remotePeer, timeout, RPCMode.AUTO);
    }

    /**
     * リモートピア上の、clz型の
     * targetIdを持つオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトのクラス
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    private <S extends RPCIf> S getStubFor(Class<? extends RPCIf> clz,
            ObjectId targetId, E remotePeer, int timeout,
            RPCMode rpcMode) {
        checkActive();
        ClassLoader loader = clz.getClassLoader();
        Class<?>[] ifs = ClassUtil.gatherLowerBoundSuperInterfaces(clz,
                RPCIf.class);
        RPCInvocationHandler<E> handler =
                new RPCInvocationHandler<E>(this, targetId, (E) remotePeer, timeout,
                        rpcMode);
        return (S) Proxy.newProxyInstance(loader, ifs, handler);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz,
            ObjectId targetId, E remotePeer, int timeout,
            RPCMode rpcMode) {
        if (!clz.isInterface()) {
            throw new IllegalArgumentException("specified class is not an interface");
        }
        return getStubFor(clz,targetId,remotePeer,timeout,rpcMode);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz,
            ObjectId targetId, E remotePeer, RPCMode rpcMode) {
        return getStub(clz,targetId,remotePeer,
                GTransConfigValues.rpcTimeout,rpcMode);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを持ち、
     * targetIdを持つオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトの型
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz,
            ObjectId targetId, E remotePeer, int timeout) {
        if (!clz.isInterface()) {
            throw new IllegalArgumentException("Specified class is not interface");
        }
        return getStubFor(clz,targetId,remotePeer,timeout,RPCMode.AUTO);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを持ち、
     * targetIdを持つオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトの型
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz,
            ObjectId targetId, E remotePeer) {
        return getStub(clz,targetId,remotePeer,
                GTransConfigValues.rpcTimeout);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public <S extends RPCIf> S getStub(Class<S> clz,
            CalleeId cid, int timeout,RPCMode rpcMode) {
        return getStub(clz,cid.getTargetId(),(E)cid.getPeerRef(),timeout,rpcMode); 
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public <S extends RPCIf> S getStub(Class<S> clz,
            CalleeId cid,RPCMode rpcMode) {
        return getStub(clz,cid.getTargetId(),(E)cid.getPeerRef(),GTransConfigValues.rpcTimeout,rpcMode); 
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを持ち、
     * cidで指定される
     * オブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトの型
     * @param ref 対象オブジェクトを指定する
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public <S extends RPCIf> S getStub(Class<S> clz,
            CalleeId cid, int timeout) {
        return getStub(clz,cid.getTargetId(),
                (E)cid.getPeerRef(),timeout);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースをを持ち、
     * cidで指定される
     * オブジェクトのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトの型
     * @param ref 対象オブジェクトを指定する
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public <S extends RPCIf> S getStub(Class<S> clz,
            CalleeId cid) {
        return getStub(clz,cid.getTargetId(),
                (E)cid.getPeerRef());
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public Object rcall(
            ObjectId targetId, E remotePeer, int timeout,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return getStub(DynamicStub.class,targetId,remotePeer,timeout,rpcMode).method(method, args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public Object rcall(
            ObjectId targetId, E remotePeer,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return rcall(targetId, remotePeer, GTransConfigValues.rpcTimeout,rpcMode,method,args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    public Object rcall(
            ObjectId targetId, E remotePeer, int timeout,
            String method, Object... args) throws Throwable {
        return rcall(targetId, remotePeer, timeout, RPCMode.AUTO,method,args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * targetIdを持つオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @return RPCのためのstub
     */
    public Object rcall(
            ObjectId targetId, E remotePeer,
            String method, Object... args) throws Throwable {
        return rcall(targetId, remotePeer, GTransConfigValues.rpcTimeout,method,args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public Object rcall(
            CalleeId cid, int timeout,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return rcall(cid.getTargetId(),(E)cid.getPeerRef(),timeout,rpcMode,method, args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public Object rcall(
            CalleeId cid,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return rcall(cid,GTransConfigValues.rpcTimeout,rpcMode,method, args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    @SuppressWarnings("unchecked")
    public Object rcall(
            CalleeId cid, int timeout,
            String method, Object... args) throws Throwable {
        return rcall(cid.getTargetId(),(E)cid.getPeerRef(),timeout,RPCMode.AUTO,method, args);
    }
    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで指定されるオブジェクトのメソッドを動的に呼び出す。
     * 
     * @param clz RPC呼び出しの対象となるオブジェクトが実装しているインターフェース
     * @param targetId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @return RPCのためのstub
     */
    public Object rcall(
            CalleeId cid,
            String method, Object... args) throws Throwable {
        return rcall(cid,GTransConfigValues.rpcTimeout,RPCMode.AUTO,method, args);
    }
    
    public void changeRPCTimeout(RPCIf stub, int timeout) throws IllegalArgumentException {
        checkActive();
        @SuppressWarnings({ "unchecked" })
        RPCInvocationHandler<E> handler =
                (RPCInvocationHandler<E>) Proxy.getInvocationHandler(stub);
        handler.setTimeout(timeout);
    }
    
    protected MethodCall newMethodCall(ObjectId target, E remotePeer,
            String method, Object... args) {
        return new MethodCall(target, peer.getPeerId(), method, args);
    }

    /**
     * oneway RPCのための送信処理を行う。
     * 
     * @param target
     * @param remotePeer
     * @param method
     * @param args
     * @throws RPCException 
     */
    public void sendOnewayInvoke(ObjectId target,
            E remotePeer, String method, Object... args) throws RPCException {
        // TODO think!
        // NoSuchPeerExceptionは返すべき否か？
        // 返す必要があるなら、channelを使う必要がある
        try {
            MethodCall mc = newMethodCall(target, remotePeer, method, args);
            trans.send(transId, remotePeer, mc);
        } catch (IOException e) {
            throw new RPCException(e);
        } 
    }

    /**
     * RPCのための送信処理を行う。
     * 返り値に例外がセットされた場合、次の例外のケースに限る。
     * RPCException - 通信処理の中で発生した例外をcauseとして保持する
     * 
     * @param target
     * @param remotePeer
     * @param timeout
     * @param method
     * @param args
     * @return RPCの結果
     * @throws RPCException 
     */
    public ReturnValue<?> sendInvoke(ObjectId target, E remotePeer, int timeout,
            String method, Object... args) throws RPCException {
        Channel<?> ch = null;
        try {
            if (POOL_CHANNEL) {
                ch = channelPool.get(remotePeer);
                if (ch == null) {
                    ch = trans.newChannel(transId, remotePeer);
                    channelPool.put(remotePeer, ch);
                }
            }
            else {
                ch = trans.newChannel(transId, remotePeer);
            }
        } catch (IOException e) {
            throw new RPCException(e);
        }
        try {
            MethodCall mc = newMethodCall(target, remotePeer, method, args);
            ch.send(mc);
            Object r = ch.receive(timeout);
            if (r == null) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RPCException(new InterruptedException());
                }
                throw new RPCException("RPC return message is null");
            }
            if (!(r instanceof ReturnValue<?>)) {
                throw new RPCException("RPC return message is not ReturnValue");
            }
            return (ReturnValue<?>)r;
        } catch (NetworkTimeoutException e) {
            throw new RPCException(new NetworkTimeoutException(method
                    + " call timed out"));
        } catch (IOException e) {
            throw new RPCException(e);
        } finally {
            if (!POOL_CHANNEL) {
                ch.close();
            }	
        }
    }
    
    protected Object invokeInReceive(boolean isOneway, RPCIf obj, MethodCall mc)
            throws NoSuchMethodException, InvocationTargetException {
        if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.calleeHook(mc.method, mc.args);
            mc.method = rv.method;
            mc.args = rv.args;
        }
        return MethodUtil.strictInvoke(obj, RPCIf.class,
                peer.getPeerId().equals(mc.srcPeerId), mc.method, mc.args);
    }

    protected void receiveOneway(MethodCall mc) {
        /*
         * callee側で発生した例外は、callerに渡すことができないため、すべて、log出力する
         */
        try {
            RPCIf obj = getRPCObject(mc.target);
            if (obj == null) {
                throw new NoSuchRemoteObjectException("no such object of ID: "
                        + mc.target + " in " + trans.getPeerId());
            } else {
             	invokeInReceive(true, obj, mc);
            }
        } catch (InvocationTargetException e) {
            logger.info("oneway RPC callee got Exception: \"{}\"", mc.method);
            logger.info("", e.getCause());
        } catch (Throwable e) {
            // any Exception or Error except for InvocationTargetException
            logger.warn("", e);
        }
    }

    protected ReturnValue<?> receiveSync(MethodCall mc) {
        ReturnValue<?> ret;
        try {
            RPCIf obj = getRPCObject(mc.target);
            if (obj == null) {
                ret = new ReturnValue<Object>(new RPCException(
                        new NoSuchRemoteObjectException(
                                "target object of ID not found: " + mc.target)));
            } else {
                ret = new ReturnValue<Object>(invokeInReceive(false, obj, mc));
            }
        } catch (InvocationTargetException e) {
            ret = new ReturnValue<Object>(e.getCause());
        } catch (Throwable e) {
            // any Exception or Error except for InvocationTargetException
            logger.error("", e);
            ret = new ReturnValue<Object>(e);
        }
        return ret;
    }

    private void _onReceive(ReceivedMessage rmsg) {
        MethodCall mc = (MethodCall) rmsg.getMessage();
        if (mc == null) {
            logger.error("null message received");
            return;
        }
        // case of call oneway
        srcPeerId.set(mc.srcPeerId);
        receiveOneway(mc);
    }
    
    private void _onReceive(Channel<E> ch) {
        if (ch.isCreatorSide()) {
            // dose nothing
            return;
        }
        Object obj = ch.receive();
        if (obj == null) {
        		logger.error("null message received");
        		return;
        }
        	if (!(obj instanceof MethodCall)) {
        		logger.info("Maybe the reply is received after the caller-channel is closed");
        		return;
        }
        MethodCall mc = (MethodCall) obj; 
        // case of call sync
        try {
            srcPeerId.set(mc.srcPeerId);
            ReturnValue<?> ret = receiveSync(mc);
            if (ch.isClosed()) {
                logger.info("channel already closed on the return of \"{}\" method",
                        mc.method);
            } else {
            	//if (ret.getException() != null) System.out.println("Exception reply to " + mc.method + ":" + ch + ",isCreator?=" + ch.isCreatorSide());
                ch.send(ret);
            }
        } catch (ClosedChannelException e) {
        		logger.warn("", e);
        		logger.info("closed channel exception occured to reply to \"{}\", args={}", mc.method, mc.args);
        } catch (IOException e) {
        		logger.warn("", e);
            logger.info("caller could not receive the return of \"{}\" method",
                    mc.method);
        }
    }
}
