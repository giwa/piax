/*
 * TcpLocator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TcpLocator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.RawTransport;

/**
 * TCPのためのPeerLocatorを表現するクラス。
 * 
 * 
 */
public class TcpLocator extends InetLocator {
    private static final long serialVersionUID = 1L;

    public TcpLocator(InetSocketAddress addr) {
        super(addr);
    }

    @Override
    public RawTransport<TcpLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return new TcpTransport(peerId, this);
    }
}
