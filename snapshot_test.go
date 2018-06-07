package raft

import (
	"testing"
	"github.com/zoo-keeper/raft/protobuf"
	"github.com/golang/protobuf/proto"
	"log"
	"fmt"
)

func TestSnaphotRequest(t *testing.T) {


	snaphosResquest := &protobuf.SnapshotRequest{LastIndex:100, LastTerm:1,LeaderName:"pro"}

	req,err := proto.Marshal(snaphosResquest); if err  != nil{
		log.Fatal("Marshal error :",err)
	}

	newReq := &protobuf.SnapshotRequest{}
	err = proto.Unmarshal(req,newReq); if err != nil{
		log.Fatal("Unmarshal error: ",err)
	}

	fmt.Printf("old index %d, new index : %d\n",snaphosResquest.LastIndex, newReq.GetLastIndex())

}