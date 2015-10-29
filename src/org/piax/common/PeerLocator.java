/*
 * PeerLocator.java - An abstract class of peer locator.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PeerLocator.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

import java.io.IOException;

import org.piax.gtrans.raw.RawTransport;

/**
 * ピアのlocatorを示す抽象クラスを定義する。
 */
public abstract class PeerLocator implements Endpoint {
    private static final long serialVersionUID = 1L;

    /**
     * このピアlocatorを使った通信をサポートするRawTransportを生成する。
     * rawListenerにはRawTransportが受信したバイト列を受け取る上位層のオブジェクトを指定する。
     * 
     * @return このピアlocatorを使った通信をサポートするRawTransport
     */
    public abstract RawTransport<? extends PeerLocator> newRawTransport(PeerId peerId)
            throws IOException;
    
    /**
     * targetに指定されたPeerLocatorオブジェクトと同一のクラスであるときに
     * trueを返す。
     * 
     * @param target 比較対象となるPeerLocatorオブジェクト
     * @return targetが同じクラスであるときtrue
     */
    public boolean sameClass(PeerLocator target) {
        return this.getClass().equals(target.getClass());
    }
}
