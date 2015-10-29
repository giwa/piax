package org.piax.samples.cityagent;

import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface CityAgentIf extends AgentIf {

    @RemoteCallable
    String getCityName();
}
