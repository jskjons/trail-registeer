package com.rei.trailregister.cluster;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.time.LocalDate;
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

import com.rei.trailregister.FileUsageRepository;
import com.rei.trailregister.UsageKey;
import com.rei.trailregister.UsageRepository;
import com.rei.trailregister.client.TrailRegisterClient;

public class ClusteredFileUsageRepository implements UsageRepository {
	private static Logger logger = LoggerFactory.getLogger(ClusteredFileUsageRepository.class);

    private ScheduledExecutorService availabilityCheckExecutor = Executors.newScheduledThreadPool(1); 
    
    private volatile List<Peer> availablePeers = Collections.emptyList();
    private List<Peer> possiblePeers;
	private UUID id;
	private volatile CountDownLatch initialized = new CountDownLatch(1);

    private FileUsageRepository delegate;
	
    public ClusteredFileUsageRepository(Path dataDir, UUID id, List<HostAndPort> peers) {
        delegate = new FileUsageRepository(dataDir);
		this.id = id;
        possiblePeers = peers.stream().map(Peer::new).collect(toList());
        availabilityCheckExecutor.scheduleAtFixedRate(this::checkAvailability, 0, 1, TimeUnit.MINUTES);
    }

    public void recordUsages(UsageKey key, int num, LocalDate date) {
        delegate.recordUsages(key, num, date);
    }

    public List<String> getApps() {
        return delegate.getApps();
    }

    public List<String> getEnvironments(String app) {
        return delegate.getEnvironments(app);
    }

    public List<String> getCategories(String app, String env) {
        return delegate.getCategories(app, env);
    }

    public List<String> getKeys(String app, String env, String category) {
        return delegate.getKeys(app, env, category);
    }
    
    @Override
    public Map<String, Long> getUsagesByDate(UsageKey key, int days) {
    	awaitInitialization();
        Map<String, Long> localResult = delegate.getUsagesByDate(key, days);
        
        availablePeers.parallelStream().forEach(peer -> 
            localResult.putAll(peer.client.getUsagesByDate(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days)));
        
        return localResult;
    }

    @Override
    public long getUsages(UsageKey key, int days) {
    	awaitInitialization();
        long localResult = delegate.getUsages(key, days);
        return localResult + getUsagesFromPeers(key, days);
    }

    public FileUsageRepository getDelegate() {
        return delegate;
    }
    
    private long getUsagesFromPeers(UsageKey key, int days) {
        return availablePeers.parallelStream()
                             .mapToLong(peer -> peer.client.getUsages(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days))
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
    		return "http://" + host.getHost() + ":" + host.getPort();
    	}
    	
    	@Override
    	public String toString() {
    		return host.toString();
    	}
    }
}
