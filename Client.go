/*
The forestbus package contains client functionality for communicating with a Forest Bus cluster.

The client handles establishing the connectivity to the individual nodes and finding the current location of leaders for given topics.
*/
package forestbus

import (
	"errors"
	//"github.com/ugorji/go/codec"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"owlfish.com/forestbus/rapi"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ErrNoNodesAvailable is a potentially transitory error that occurs when the client is unable to connect succesfully to any node in the cluster.
var ErrNoNodesAvailable = errors.New("Unable to connect to any of the given nodes.")

// ErrClusterIdMismatch is a serious error where the nodes given do not identify with the same clusterID used to setup the Client object.
var ErrClusterIdMismatch = errors.New("The nodes cluster ID does not match that given by the client.")

// ErrTopicNotFound is returned when the given topic could not be found on the node.
var ErrTopicNotFound = errors.New("Topic not found on the node.")

var err_unable_to_connect_to_node = errors.New("Unable to connect to this node.")

type rpcClientFactory func() (*rpc.Client, error)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var forestConnectRegex = regexp.MustCompile(`^([^#@]*)?#?([^@]+)?@(.*)`)

/*
ParseConnectionString takes a connection string and returns consituent parts:

	Connection string format is: clusterID#topic@nodelist

	clusterID is the optional ID of the cluster
	#topic is the optional name of the topic
	@nodelist is list of one or more nodes, comma separated.

The return result is the cluster ID, topic and a slice of node connection strings.
*/
func ParseConnectionString(constr string) (clusterID string, topic string, nodes []string) {
	regResult := forestConnectRegex.FindAllStringSubmatch(constr, -1)
	if len(regResult) != 1 || len(regResult[0]) != 4 {
		return "", "", nil
	}
	clusterID = regResult[0][1]
	topic = regResult[0][2]
	nodes = strings.Split(regResult[0][3], ",")
	return clusterID, topic, nodes
}

/*
ClientConfiguration functions are able to configure options on a Client.

The ClientConfiguration type is exposed externally to facilitate building a list of options programmatically, e.g.:

	options := make ([]forestbus.ClientConfiguration,0)
	options = append (options, forestbus.ClientEnableDebug())
	options = append (options, forestbus.ClientPreferredNode ("localhost:3000"))
	client := forestbus.NewClient ("testcluster", []string{"localhost:3000,localhost:3001"}, options...)
*/
type ClientConfiguration func(*Client)

/*
ClientEnableDebug returns a ClientConfiguration that can be passed to NewClient to enable debug loggging in the library.

For example:

	client := forestbus.NewClient ("testcluster", []string{"localhost:3000"}, forestbus.ClientEnableDebug())

*/
func ClientEnableDebug() ClientConfiguration {
	return func(client *Client) {
		client.debug = true
	}
}

/*
ShuffleNodes randomises the order in which connections attempts are made to the nodes that have been given.  Use of this configuration options helps spread the load of different clients that are doing GetMessages calls across the cluster.
*/
func ShuffleNodes() ClientConfiguration {
	return func(client *Client) {
		// Shuffle the nodes list so that clients are distributed evenly
		for i, _ := range client.nodes {
			j := rand.Intn(i + 1)
			client.nodes[i], client.nodes[j] = client.nodes[j], client.nodes[i]
		}
	}
}

/*
The Client object is a multi-goroutine safe object for communicating with the cluster of nodes that form the Forest Bus.

Each Client establishes an underlying rpc.Client connection to the nodes as required.  As such there may be some contention on the connection between parallel calls.
*/
type Client struct {
	clusterID string
	nodes     []string
	debug     bool

	// The nodeClientLock just protectes the nodeClientMap
	nodeClientLock sync.RWMutex
	nodeClientMap  map[string]*rpc.Client

	// The lastUsedGetLock protects the lastUsedGetNode
	lastUsedGetLock sync.RWMutex
	lastUsedGetNode string

	// The topicLeaderLock protects the topicLeaderMap
	topicLeaderLock sync.RWMutex
	topicLeaderMap  map[string]string
}

/*
NewClient returns a Client object for connecting to a Forst Bus cluster.

The clusterID must match the clusterID that each of the given nodes was started with.
The nodes list should contain every node in the cluster for full failover.  At least one node must be provided.
*/
func NewClient(clusterID string, nodes []string, config ...ClientConfiguration) *Client {
	client := &Client{}
	client.nodeClientMap = make(map[string]*rpc.Client)
	client.topicLeaderMap = make(map[string]string)
	client.clusterID = clusterID
	client.nodes = make([]string, len(nodes))
	copy(client.nodes, nodes)

	// Apply configuration now
	for _, cfg := range config {
		cfg(client)
	}

	if client.debug {
		log.Printf("Client configured for cluster %v with nodes %v\n", clusterID, nodes)
	}
	return client
}

/*
GetMessages returns a list of messages from the cluster.

If the cluster has been completely shutdown and restarted (rather than a rolling restart of individual nodes) then the commit index may be zero, in which case zero messages will be returned and the nextID will be set to zero.  Zero messages will also be returned if the topic contains no messages.  Once a message has been sent to the cluster in this topic the commit index will be recalculated and GetMessages will return as normal.

GetMessages will usually return more or fewer messages than the quantity requested.  This ensures effeicient message retrieval from the node as messages are aligned to offset and cache boundaries.  If any messages are available at the requested index then at least one message will be returned.

The first message in the msgs slice has an ID of index.  The last message in the slice has ID of nextID - 1.

If the index requested is no longer available on this node (i.e. clean-up has removed old data) then zero messages will be returned and the nextID will be the index of the first available message.

If the messages returned bring the client up to the end of the available messages, the nextID will contain the index of what will become the next message when it has been sent.  By setting wait to True and passing in the index returned by nextID, GetMessages will block until at least one new message is available, before returning that message/messages.
*/
func (client *Client) GetMessages(topic string, index int64, quantity int, wait bool) (msgs [][]byte, nextID int64, err error) {
	factory := client.rpcConnect()
	// Try all nodes for a non-error response.
	for {
		rpcclient, err := factory()
		if err != nil {
			// If the factory hits an error we have to return it.
			if client.debug {
				log.Printf("GetMessages attempt to connect to peer resulted in error %v\n", topic, err)
			}
			return nil, 0, err
		}
		// This should always be true
		if rpcclient != nil {
			// Try the get
			args := &rapi.ReceiveMessagesArgs{Topic: topic, ID: index, Quantity: quantity, WaitForMessages: wait}
			reply := &rapi.ReceiveMessagesResults{}
			err = rpcclient.Call("RPCHandler.ReceiveMessages", args, reply)
			if err == nil {
				if client.debug {
					log.Printf("ReceiveMessages RPC Call (%v) returned result %v with no error\n", args, reply)
				}
				switch reply.Result.Code {
				case rapi.RI_SUCCESS:
					return reply.ReceievedMessages, reply.NextID, nil
				case rapi.RI_TOPIC_NOT_FOUND:
					return nil, 0, ErrTopicNotFound
				}
				// For any other error we will try the next node.
			} else {
				// This must be a network error - take this connection out of circulation
				if client.debug {
					log.Printf("ReceiveMessages RPC Call (%v) returned error %v\n", err)
				}
				client.closeBrokenConnection(rpcclient)
			}
		}
	}
}

/*
GetTopicMaxAvailableIndex returns the maximum available index from the currently connected node for the given topic.

If the cluster has been completely shutdown and restarted (rather than a rolling restart of individual nodes) then the commit index may be zero, in which case the maxAvailableIndex will be zero.  Once a message has been sent to the cluster in this topic the commit index will be recalculated and the maximum commit index will return as normal.
*/
func (client *Client) GetTopicMaxAvailableIndex(topic string) (maxAvailableIndex int64, err error) {
	factory := client.rpcConnect()
	// Try all nodes for a non-error response.
	for {
		rpcclient, err := factory()
		if err != nil {
			// If the factory hits an error we have to return it.
			if client.debug {
				log.Printf("GetTopicMaxAvailableIndex attempt to connect to peer resulted in error %v\n", topic, err)
			}
			return 0, err
		}
		// This should always be true
		if rpcclient != nil {
			// Try the get
			args := &rapi.GetTopicDetailsArgs{Topic: topic}
			reply := &rapi.GetTopicDetailsResults{}
			err = rpcclient.Call("RPCHandler.GetTopicDetails", args, reply)
			if err == nil {
				if client.debug {
					log.Printf("GetTopicDetails RPC Call (%v) returned result %v with no error\n", args, reply)
				}
				switch reply.Result.Code {
				case rapi.RI_SUCCESS:
					return reply.CommitIndex, nil
				case rapi.RI_TOPIC_NOT_FOUND:
					return 0, ErrTopicNotFound
				}
				// For any other error we will try the next node.
			} else {
				// This must be a network error - take this connection out of circulation
				if client.debug {
					log.Printf("GetTopicDetails RPC Call returned error %v\n", err)
				}
				client.closeBrokenConnection(rpcclient)
			}
		}
	}
}

/*
SendMessages sends a batch of messages to the Forest Bus cluster.

Messages are a slice of slices of bytes.  Sending many messages (hundreds) at once gives better through-put than sending individual messages.  To easily batch messages together in this way please see the MessageBatcher documentation.

If waitForCommit is false then SendMessages will return as soon as the message has been saved on the leader node for this topic.  If waitForCommit is true then SendMessages will only return once the messages have been replicated to a majority of the nodes in the cluster and are therefore committed.
*/
func (client *Client) SendMessages(topic string, messages [][]byte, waitForCommit bool) (indexes []int64, err error) {
	factory := client.rpcConnectToTopicLeader(topic)
	// Try all nodes for a non-error response.
	for {
		rpcclient, err := factory()
		if err != nil {
			// If the factory hits an error we have to return it.
			if client.debug {
				log.Printf("SendMessages attempt to connect to peer for topic %v resulted in error %v\n", topic, err)
			}
			return nil, err
		}
		// This should always be true
		if rpcclient != nil {
			// Try the get
			args := &rapi.SendMessagesArgs{Topic: topic, SentMessages: messages, WaitForCommit: waitForCommit}
			reply := &rapi.SendMessagesResults{}

			err = rpcclient.Call("RPCHandler.SendMessages", args, reply)
			if err == nil {
				if client.debug {
					log.Printf("SendMessages RPC Call (%v) returned result %v with no error\n", args, reply)
				}
				switch reply.Result.Code {
				case rapi.RI_SUCCESS:
					return reply.IDs, nil
				case rapi.RI_TOPIC_NOT_FOUND:
					return nil, ErrTopicNotFound
				case rapi.RI_NODE_NOT_LEADER:
					// TODO - enable feeding in the last known nodename to the connection factory?
				}
				// For any other error we will try the next node.
			} else {
				// This must be a network error - take this connection out of circulation
				if client.debug {
					log.Printf("SendMessages RPC Call returned with error: %v\n", err)
				}
				client.closeBrokenConnection(rpcclient)
			}
		}
	}
}

/*
Close closes down all RPC connections that are currently in use.  Further calls to the Client will reopen connections as required.
*/
func (client *Client) Close() {
	client.nodeClientLock.RLock()
	for nodeName, rpcClient := range client.nodeClientMap {
		// We ignore any errors
		rpcClient.Close()
		delete(client.nodeClientMap, nodeName)
	}
	client.nodeClientLock.RUnlock()
}

func (client *Client) rpcConnectToTopicLeader(topic string) rpcClientFactory {
	client.topicLeaderLock.RLock()
	topicLeader := client.topicLeaderMap[topic]
	client.topicLeaderLock.RUnlock()

	robinConnector := client.roundRobinConnector(topicLeader)

	return func() (*rpc.Client, error) {
		nodeName, result, err := robinConnector()
		if err == nil {
			if topicLeader == "" || topicLeader != nodeName {
				client.setTopicLeader(nodeName, topic)
			}
			return result, nil
		}
		return nil, err
	}
}

func (client *Client) rpcConnect() rpcClientFactory {
	client.lastUsedGetLock.RLock()
	// If we don't know what the last known good one was, start with the first.
	firstNodeName := client.lastUsedGetNode
	client.lastUsedGetLock.RUnlock()

	robinConnector := client.roundRobinConnector(firstNodeName)

	return func() (*rpc.Client, error) {
		nodeName, result, err := robinConnector()
		if err == nil {
			if firstNodeName == "" || nodeName != firstNodeName {
				client.setLastUsed(nodeName)
			}
			return result, nil
		}
		return nil, err
	}
}

func (client *Client) roundRobinConnector(firstNodeName string) func() (nodeName string, rpcclient *rpc.Client, err error) {
	lastTried := 0
	nodesToTry := make([]string, len(client.nodes))
	copy(nodesToTry, client.nodes)
	// Prioritise the firstNodeName
	firstIndex := getSlicePosition(firstNodeName, nodesToTry)
	if firstIndex > -1 {
		nodesToTry[0], nodesToTry[firstIndex] = nodesToTry[firstIndex], nodesToTry[0]
	}

	return func() (string, *rpc.Client, error) {
		var result *rpc.Client
		var err error
		// We are not holding the lock because client.nodes never changes
		for lastTried < len(nodesToTry) {
			nodeName := nodesToTry[lastTried]
			lastTried++
			// We have a different node to try.
			result, err = client.getRPCClientForNode(nodeName)
			if err == nil {
				return nodeName, result, nil
			}
			// We have an error - which one?
			if err == ErrClusterIdMismatch {
				// We always bail on a mismath rather than trying other nodes
				return "", nil, err
			}
		}
		// We've run out of nodes to try and nothing is working
		return "", nil, ErrNoNodesAvailable
	}
}

/*
closeBrokenConnection is an internal method to remove broken connections from our cache.
*/
func (client *Client) closeBrokenConnection(rpcclient *rpc.Client) {
	client.nodeClientLock.Lock()
	defer client.nodeClientLock.Unlock()
	rpcclient.Close()
	for key, value := range client.nodeClientMap {
		if value == rpcclient {
			delete(client.nodeClientMap, key)
			return
		}
	}
}

/*
getRPCClientForNode is an internal method that tries to use the cached rpc instance if it's available.

If an instance is not found in the map then a write lock is acquired and a new connection attempted.
*/
func (client *Client) getRPCClientForNode(nodename string) (*rpc.Client, error) {
	var rpcclient *rpc.Client
	var err error
	client.nodeClientLock.RLock()
	rpcclient, ok := client.nodeClientMap[nodename]

	client.nodeClientLock.RUnlock()
	if ok {
		return rpcclient, nil
	}
	// No already open connection - we need to create one.
	client.nodeClientLock.Lock()
	//rpcclient, err = rpc.Dial("tcp", nodename)
	netconn, err := net.Dial("tcp", nodename)

	if err != nil {
		// Error - unable to connect to this node.
		client.nodeClientLock.Unlock()
		return nil, err_unable_to_connect_to_node
	}
	rpcclient = rpc.NewClient(netconn)
	//var handlerCodec codec.CborHandle
	//rpcCodec := codec.GoRpc.ClientCodec(netconn, &handlerCodec)
	//rpcclient = rpc.NewClientWithCodec(rpcCodec)

	// Check cluster ID's match
	args := &rapi.GetClusterDetailsArgs{}
	reply := &rapi.GetClusterDetailsResults{}
	err = rpcclient.Call("RPCHandler.GetClusterDetails", args, reply)
	if err == nil && reply.Result.Code == rapi.RI_SUCCESS {
		// Check the cluster IDs match
		if client.clusterID != reply.ClusterID {
			// This is a serious enough issue that we bail with a fatal error
			rpcclient.Close()
			client.nodeClientLock.Unlock()
			return nil, ErrClusterIdMismatch
		}
		// We have a valid connection - store it.
		client.nodeClientMap[nodename] = rpcclient
		client.nodeClientLock.Unlock()
		return rpcclient, nil
	}
	// The call failed - so close the connection and return an error
	rpcclient.Close()
	client.nodeClientLock.Unlock()
	return nil, err_unable_to_connect_to_node

}

func (client *Client) setLastUsed(nodename string) {
	client.lastUsedGetLock.Lock()
	client.lastUsedGetNode = nodename
	client.lastUsedGetLock.Unlock()
}

func (client *Client) setTopicLeader(nodename string, topic string) {
	client.topicLeaderLock.Lock()
	client.topicLeaderMap[topic] = nodename
	client.topicLeaderLock.Unlock()
}

func getSlicePosition(search string, list []string) int {
	for i, strval := range list {
		if strval == search {
			return i
		}
	}
	return -1
}
