package example.gossip;

import java.util.ArrayList;
import java.util.List;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Linkable;
import peersim.core.Node;
import peersim.core.Protocol;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import peersim.transport.UniformRandomTransport;


/**
 * @author Lucas Provensi
 * 
 * Basic Shuffling protocol template
 * 
 * The basic shuffling algorithm, introduced by Stavrou et al in the paper: 
 * "A Lightweight, Robust P2P System to Handle Flash Crowds", is a simple 
 * peer-to-peer communication model. It forms an overlay and keeps it 
 * connected by means of an epidemic algorithm. The protocol is extremely 
 * simple: each peer knows a small, continuously changing set of other peers, 
 * called its neighbors, and occasionally contacts a random one to exchange 
 * some of their neighbors.
 * 
 * This class is a template with instructions of how to implement the shuffling
 * algorithm in PeerSim.
 * Should make use of the classes Entry and GossipMessage:
 *    Entry - Is an entry in the cache, contains a reference to a neighbor node
 *  		  and a reference to the last node this entry was sent to.
 *    GossipMessage - The message used by the protocol. It can be a shuffle
 *    		  request, reply or reject message. It contains the originating
 *    		  node and the shuffle list.
 *
 */
public class BasicShuffle  implements Linkable, EDProtocol, CDProtocol{
	
	private static final String PAR_CACHE = "cacheSize";
	private static final String PAR_L = "shuffleLength";
	private static final String PAR_TRANSPORT = "transport";

	private final int tid;

	// The list of neighbors known by this node, or the cache.
	private List<Entry> cache;
	private List<Entry> tempCache;

	
	// The maximum size of the cache;
	private final int size;
	
	// The maximum length of the shuffle exchange;
	private final int l;
	
	private boolean waiting_for_response;
	
	/**
	 * Constructor that initializes the relevant simulation parameters and
	 * other class variables.
	 * 
	 * @param n simulation parameters
	 */
	public BasicShuffle(String n)
	{
		this.size = Configuration.getInt(n + "." + PAR_CACHE);
		this.l = Configuration.getInt(n + "." + PAR_L);
		this.tid = Configuration.getPid(n + "." + PAR_TRANSPORT);
		waiting_for_response = false;
		cache = new ArrayList<Entry>(size);
		tempCache = new ArrayList<Entry>(size);

	}

	/* START YOUR IMPLEMENTATION FROM HERE
	 * 
	 * The simulator engine calls the method nextCycle once every cycle 
	 * (specified in time units in the simulation script) for all the nodes.
	 * 
	 * You can assume that a node initiates a shuffling operation every cycle.
	 * 
	 * @see peersim.cdsim.CDProtocol#nextCycle(peersim.core.Node, int)
	 */
	@Override
	public void nextCycle(Node node, int protocolID) {
		// Implement the shuffling protocol using the following steps (or
		// you can design a similar algorithm):
		// Let's name this node as P
		
		// 1. If P is waiting for a response from a shuffling operation initiated in a previous cycle, return;
		//System.out.println("waiting :" + waiting_for_response);
		if(waiting_for_response) return;
		
		// 2. If P's cache is empty, return;	
		if(cache.size() == 0) return;
		
		
		// 3. Select a random neighbor (named Q) from P's cache to initiate the shuffling;
		//	  - You should use the simulator's common random source to produce a random number: CommonState.r.nextInt(cache.size())
		int qnum = CommonState.r.nextInt(cache.size());
		Node q = cache.get(qnum).getNode();
		
		// 4. If P's cache is full, remove Q from the cache;
		tempCache = new ArrayList<Entry>(cache);
		cache.remove(qnum);
		tempCache.remove(qnum);
		
		
		if(cache.size() > size){
			throw new RuntimeException("cry is free");
		}
		
		// 5. Select a subset of other l - 1 random neighbors from P's cache;
		//	  - l is the length of the shuffle exchange
		//    - Do not add Q to this subset	
		int subsetSize = l - 1;
		Entry selected = null;
	
		ArrayList<Entry> subset = new ArrayList<Entry>(subsetSize);
		while(subset.size() < subsetSize && tempCache.size() > 0) {
			if (cache.size() == 0) {
				break;
			}
			int sel = CommonState.r.nextInt(tempCache.size());
			selected = tempCache.remove(sel);
			if(selected.getNode() == q || subset.contains(selected)) {
				continue;
			}
			subset.add(selected);
			selected.setSentTo(q);
		}
		
		// 6. Add P to the subset;
		subset.add(new Entry(node));
		
		// 7. Send a shuffle request to Q containing the subset;
		//	  - Keep track of the nodes sent to Q
		//	  - Example code for sending a message:
		//
		GossipMessage message = new GossipMessage(node, subset);
		message.setType(MessageType.SHUFFLE_REQUEST);
		Transport tr = (Transport) node.getProtocol(tid);

		tr.send(node, q, message, protocolID);
		waiting_for_response = true;
		//System.out.println("eewew");
		//
		// 8. From this point on P is waiting for Q's response and will not initiate a new shuffle operation;
		//
		// The response from Q will be handled by the method processEvent.
		
	}

	/* The simulator engine calls the method processEvent at the specific time unit that an event occurs in the simulation.
	 * It is not called periodically as the nextCycle method.
	 * 
	 * You should implement the handling of the messages received by this node in this method.
	 * 
	 * @see peersim.edsim.EDProtocol#processEvent(peersim.core.Node, int, java.lang.Object)
	 */
	@Override
	public void processEvent(Node node, int pid, Object event) {
		// Let's name this node as Q;
		// Q receives a message from P;
		//	  - Cast the event object to a message:
		GossipMessage message = (GossipMessage) event;
		//System.out.println(message.getType());
		switch (message.getType()) {
		// If the message is a shuffle request:
		//	  1. If Q is waiting for a response from a shuffling initiated in a previous cycle, send back to P a message rejecting the shuffle request;			

		case SHUFFLE_REQUEST:
			
			if(this.waiting_for_response) {
				GossipMessage to_send = new GossipMessage(node, new ArrayList<Entry>());
				to_send.setType(MessageType.SHUFFLE_REJECTED);
				Transport tr = (Transport) node.getProtocol(tid);
				tr.send(node, message.getNode(), to_send, pid);
				return;
			}
			
		//	  2. Q selects a random subset of size l of its ownownownownownownownownownown neighbors; 
			Node p = message.getNode();
			int subsetSize = l;
			Entry selected = null;
			
			
			ArrayList<Entry> subset = new ArrayList<Entry>(subsetSize);
			tempCache = new ArrayList<Entry>(cache);

			while(subset.size() < subsetSize && tempCache.size() > 0) {
				
				int sel = CommonState.r.nextInt(tempCache.size());
				selected = tempCache.remove(sel);
				if(selected.getNode() == p || subset.contains(selected)) {
					continue;
				}
				selected.setSentTo(p);
				subset.add(selected);
			}
						
			
		//	  3. Q reply P's shuffle request by sending back its own subset;
			GossipMessage messageRepl = new GossipMessage(node, subset);
			messageRepl.setType(MessageType.SHUFFLE_REPLY);
			Transport tr = (Transport) node.getProtocol(tid);
			tr.send(node, p, messageRepl, pid);
			waiting_for_response = false;
		//	  4. Q updates its cache to include the neighbors sent by P:
		//		 - No neighbor appears twice in the cache
		//		 - Use empty cache slots to add the new entries
		//		 - If the cache is full, you can replace entries among the ones sent to P with the new ones
			int j = 0;
			for(Entry e: message.getShuffleList()) {
				Node test = e.getNode();
				BasicShuffle bs = (BasicShuffle) test.getProtocol(pid);
				
				if(bs.contains(node)) continue;
				
				if(cache.contains(e)) {
					continue;
				}
				
				else if(cache.size() == size) {
					for(; j < cache.size(); j ++) {
						if(cache.get(j).getSentTo() == p) {
							cache.set(j, e);
							e.setSentTo(null);
							break;
						}
					}
				}
				
				else {
					cache.add(e);
				}
			}
			
			break;
		
		// If the message is a shuffle reply:
		case SHUFFLE_REPLY:
		//	  1. In this case Q initiated a shuffle with P and is receiving a response containing a subset of P's neighbors
		//	  2. Q updates its cache to include the neighbors sent by P:
			j = 0;
			for(Entry e: message.getShuffleList()) {
				Node test = e.getNode();
				BasicShuffle bs = (BasicShuffle) test.getProtocol(pid);
				
				if(bs.contains(node)) continue;
						
				
				if(cache.contains(e)) {
					continue;
				}
				
				else if(cache.size() == size) {
					for(; j < cache.size(); j ++) {
						if(cache.get(j).getSentTo() == message.getNode()) {
							cache.set(j, e);
							cache.get(j).setSentTo(null);
							break;
						}
					}
				}
				
				else {
					cache.add(e);
				}
			}
		//		 - No neighbor appears twice in the cache
		//		 - Use empty cache slots to add new entries
		//		 - If the cache is full, you can replace entries among the ones originally sent to P with the new ones
		//	  3. Q is no longer waiting for a shuffle reply;
			for(Entry e: cache) {
				e.setSentTo(null);
			}
			
			waiting_for_response = false;
			break;
		
		// If the message is a shuffle rejection:
		case SHUFFLE_REJECTED:
			
		//	  1. If P was originally removed from Q's cache, add it again to the cache.
		//	  2. Q is no longer waiting for a shuffle reply;
			if(!contains(message.getNode())) {
				cache.add(new Entry(message.getNode()));
			}
			
			for(Entry e: cache) {
				e.setSentTo(null);
			}
			waiting_for_response = false;
			break;
			
		default:
			break;
		}
		
	}
	
/* The following methods are used only by the simulator and don't need to be changed */
	
	@Override
	public int degree() {
		return cache.size();
	}

	@Override
	public Node getNeighbor(int i) {
		return cache.get(i).getNode();
	}

	@Override
	public boolean addNeighbor(Node neighbour) {
		if (contains(neighbour))
			return false;

		if (cache.size() >= size)
			return false;

		Entry entry = new Entry(neighbour);
		cache.add(entry);

		return true;
	}

	@Override
	public boolean contains(Node neighbor) {
		return cache.contains(new Entry(neighbor));
	}

	public Object clone()
	{
		BasicShuffle gossip = null;
		try { 
			gossip = (BasicShuffle) super.clone(); 
		} catch( CloneNotSupportedException e ) {
			
		} 
		gossip.cache = new ArrayList<Entry>();

		return gossip;
	}

	@Override
	public void onKill() {
		// TODO Auto-generated method stub		
	}

	@Override
	public void pack() {
		// TODO Auto-generated method stub	
	}
}
