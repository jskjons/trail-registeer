package com.rei.trailregister;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.rei.trailregister.client.TrailRegisterClient;

public class ClusteredUsageRepository extends UsageRepository {
	private static Logger logger = LoggerFactory.getLogger(ClusteredUsageRepository.class);

    private ScheduledExecutorService availabilityCheckExecutor = Executors.newScheduledThreadPool(1); 
    
    private volatile List<Peer> availablePeers = Collections.emptyList();
    private List<Peer> possiblePeers;
	private UUID id;
	private volatile CountDownLatch initialized = new CountDownLatch(1);
	
    public ClusteredUsageRepository(Path dataDir, UUID id, List<HostAndPort> peers) {
        super(dataDir);
		this.id = id;
        this.possiblePeers = peers.stream().map(Peer::new).collect(toList());
        availabilityCheckExecutor.scheduleAtFixedRate(this::checkAvailability, 0, 1, TimeUnit.MINUTES);
    }
    
    @Override
    public Map<String, Integer> getUsagesByDate(GetUsagesRequest req) {
    	awaitInitialization();
        Map<String, Integer> localResult = super.getUsagesByDate(req);
        if (!req.fromPeer) {
            availablePeers.parallelStream().forEach(peer -> 
                localResult.putAll(peer.client.getUsagesByDate(req.app, req.env, req.category, req.key)));
        }
        return localResult;
    }

    
    @Override
    public int getUsages(GetUsagesRequest req) {
    	awaitInitialization();
        int localResult = super.getUsages(req);
        return localResult + (!req.fromPeer ? getUsagesFromPeers(req) : 0);
    }

    private int getUsagesFromPeers(GetUsagesRequest req) {
        return availablePeers.parallelStream()
                              .mapToInt(peer -> peer.client.getUsages(req.app, req.env, req.category, req.key, req.days))
                              .sum();
    }
    
    void checkAvailability() {
    	List<Peer> peersBefore = availablePeers;
        availablePeers = possiblePeers.stream().filter(Peer::isAvailable).collect(toList());
        if (!peersBefore.equals(availablePeers)) {
        	logger.info("available peers changed to: {}", availablePeers);
        }
        
        initialized.countDown();
    }
    
    private void awaitInitialization() {
    	try {
    		initialized.await();
    	} catch (InterruptedException e) {
    		// nevermind then, don't wait for initialization...
    	}
    }
    
    private class Peer {
    	private HostAndPort host;
    	private TrailRegisterClient client; 
    	
    	public Peer(HostAndPort host) {
    		this.host = host;
    		client = new ClusterAwareTrailRegisterClient(getBaseUrl()); 
		}
    	
    	boolean isAvailable() {
    		try {
    			String pingResult = client.ping();
    			return pingResult != null && !pingResult.equals(id.toString());
    		} catch (RuntimeException e) {
    			return false;
    		}
    	}
    	
    	private String getBaseUrl() {
    		return "http://" + host.getHostText() + ":" + host.getPort();
    	}
    	
    	@Override
    	public String toString() {
    		return host.toString();
    	}
    }
}
