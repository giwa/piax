/*
 * EmuLocator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: EmuLocator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.emu;

import java.io.IOException;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.raw.RawTransport;

/**
 * 
 */
public class EmuLocator extends PeerLocator {
    private static final long serialVersionUID = 1L;

    private final int vport;
    
    public EmuLocator(int vport) {
        this.vport = vport;
    }
    
    public int getVPort() {
        return vport;
    }

    @Override
    public RawTransport<EmuLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return new EmuTransport(peerId, this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof EmuLocator) {
            return vport == ((EmuLocator) obj).vport;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return vport;
    }
    
    @Override
    public String toString() {
        return "" + vport;
    }
}
