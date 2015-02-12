/*
The rapi package contains the Go RPC method arguments and return values used by both the Forest Bus Server and Client implementations.

This package should not normally be used directly, instead please refer to the forestbus package for the library client.
*/
package rapi

import (
	"errors"
	"fmt"
)

type ResultInfo struct {
	Code        int
	Description string
	Extra1      string
}

const (
	RI_SUCCESS int = iota
	RI_MISMATCHED_CLUSTER_ID
	RI_TOPIC_NOT_FOUND
	RI_INTERNAL_ERROR
	RI_NODE_NOT_LEADER
	RI_NODE_IN_SHUTDOWN
)

func Get_RI_SUCCESS() ResultInfo {
	return ResultInfo{Code: RI_SUCCESS, Description: "Success"}
}

func Get_RI_MISMATCHED_CLUSTER_ID(nodeName, givenID, configuredID string) ResultInfo {
	return ResultInfo{Code: RI_MISMATCHED_CLUSTER_ID, Description: fmt.Sprintf("Provided Cluster ID (%v) did not match node (%v)'s ID configuration (%v).", givenID, nodeName, configuredID)}
}

func Get_RI_TOPIC_NOT_FOUND(nodeName, givenTopic string) ResultInfo {
	return ResultInfo{Code: RI_TOPIC_NOT_FOUND, Description: fmt.Sprintf("Provided Topic ID (%v) was not found on node (%v).", givenTopic, nodeName)}
}

func Get_RI_INTERNAL_ERROR(nodeName, errStr string, extra ...string) ResultInfo {
	if len(extra) == 0 {
		return ResultInfo{Code: RI_INTERNAL_ERROR, Description: fmt.Sprintf("Error (%v) occured while processing this request on node (%v).", errStr, nodeName)}
	} else {
		return ResultInfo{Code: RI_INTERNAL_ERROR, Description: fmt.Sprintf("Error (%v) occured while processing this request on node (%v).  Technical error: %v", errStr, nodeName, extra)}
	}
}

func Get_RI_NODE_NOT_LEADER(nodeName, actualLeader string) ResultInfo {
	return ResultInfo{Code: RI_NODE_NOT_LEADER, Description: fmt.Sprintf("Node (%v) is not the leader for this topic.  Last seen leader is (%v).", nodeName, actualLeader), Extra1: actualLeader}
}

func Get_RI_NODE_IN_SHUTDOWN(nodeName string) ResultInfo {
	return ResultInfo{Code: RI_NODE_IN_SHUTDOWN, Description: fmt.Sprintf("Node (%v) is shutting down.", nodeName)}
}

var ERR_TOPIC_NOT_FOUND = errors.New("Topic not found on server.")

type SendMessagesArgs struct {
	ClusterID     string
	Topic         string
	SentMessages  [][]byte
	WaitForCommit bool
}

type SendMessagesResults struct {
	Result ResultInfo
	IDs    []int64
}

type ReceiveMessagesArgs struct {
	ClusterID       string
	Topic           string
	ID              int64
	Quantity        int
	WaitForMessages bool
}

type ReceiveMessagesResults struct {
	Result            ResultInfo
	ReceievedMessages [][]byte
	NextID            int64
}

type GetClusterDetailsArgs struct {
}

type GetClusterDetailsResults struct {
	Result    ResultInfo
	ClusterID string
	Peers     []string
	Topics    []string
}

type GetTopicDetailsArgs struct {
	Topic string
}

type GetTopicDetailsResults struct {
	Result      ResultInfo
	FirstIndex  int64
	LastIndex   int64
	CommitIndex int64
}
