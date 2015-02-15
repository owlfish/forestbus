package forestbus

import (
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	var clusterID, topic string
	var nodes []string

	// Test just hosts
	clusterID, topic, nodes = ParseConnectionString("")
	if clusterID != "" || topic != "" || nodes != nil {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	clusterID, topic, nodes = ParseConnectionString("@:1010")
	if clusterID != "" || topic != "" || nodes[0] != ":1010" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	clusterID, topic, nodes = ParseConnectionString("@host:1010")
	if clusterID != "" || topic != "" || nodes[0] != "host:1010" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	clusterID, topic, nodes = ParseConnectionString("@host:1010,:2020")
	if clusterID != "" || topic != "" || nodes[0] != "host:1010" || nodes[1] != ":2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	clusterID, topic, nodes = ParseConnectionString("@:2020,host:1010")
	if clusterID != "" || topic != "" || nodes[1] != "host:1010" || nodes[0] != ":2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	// Test topic and hosts
	clusterID, topic, nodes = ParseConnectionString("#@host:1010,:2020")
	if clusterID != "" || topic != "" || nodes[0] != "host:1010" || nodes[1] != ":2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	clusterID, topic, nodes = ParseConnectionString("#t1@host:1010,:2020")
	if clusterID != "" || topic != "t1" || nodes[0] != "host:1010" || nodes[1] != ":2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	// Test clusterID, no topic
	clusterID, topic, nodes = ParseConnectionString("t1@host:1010,:2020")
	if clusterID != "t1" || topic != "" || nodes[0] != "host:1010" || nodes[1] != ":2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}

	// Test clusterID and topic
	clusterID, topic, nodes = ParseConnectionString("t1#t2@host:1010,otherhost:2020")
	if clusterID != "t1" || topic != "t2" || nodes[0] != "host:1010" || nodes[1] != "otherhost:2020" {
		t.Errorf("Unexpected result: %v, %v, %v\n", clusterID, topic, nodes)
	}
}
