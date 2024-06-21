package com.rei.trailregister.cluster;

import static com.rei.trailregister.cluster.ClusterUtils.PEERS_SEE_OTHER_VAR;
import static com.rei.trailregister.cluster.ClusterUtils.parseHostAndPorts;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

public class ClusterUtilsTest {
    @Test
    public void canParsePeers() {
        List<HostAndPort> peers = parseHostAndPorts(ImmutableMap.of(PEERS_SEE_OTHER_VAR, "OTHER", "OTHER", "hosta[4567],hostb:123"));
        assertEquals(2, peers.size());
        assertEquals("hosta", peers.get(0).getHost());
        assertEquals(4567, peers.get(0).getPort());
        
        assertEquals("hostb", peers.get(1).getHost());
        assertEquals(123, peers.get(1).getPort());
    }

}
