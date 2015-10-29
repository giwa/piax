/*
 * OverlayListener.java - A listener for overlays.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: OverlayListener.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov;

import org.piax.common.Destination;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.TransportListener;

/**
 * OverlayListener is the listener for the Overlays.
 * 
 * OverlayListener has same signature as TransportListener.
 * It is needed to implement onReceive of TransportListener (but never called on this class).
 */
public interface OverlayListener<D extends Destination, K extends Destination> 
	extends TransportListener<D> {
    void onReceive(Overlay<D, K> ov, OverlayReceivedMessage<K> rmsg);
    
    FutureQueue<?> onReceiveRequest(Overlay<D, K> ov,
            OverlayReceivedMessage<K> rmsg);

    /* Java 8
    default public void onReceive(Transport<D> trans, ReceivedMessage rmsg) {
    }*/
}
