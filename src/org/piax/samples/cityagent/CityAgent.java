package org.piax.samples.cityagent;

import org.piax.agent.MobileAgent;

public class CityAgent extends MobileAgent implements CityAgentIf {
    private static final long serialVersionUID = 1L;

    public String getCityName() {
        return (String) this.getAttribValue("city");
    }

    @Override
    public void onCreation() {
        System.out.printf("Hello! peer:%s agId:%s name:%s%n", getHome()
                .getPeerId(), getId(), getName());
    }

    @Override
    public void onDestruction() {
        System.out.printf("Bye! peer:%s name:%s%n", getHome().getPeerId(),
                getName());
    }
}
