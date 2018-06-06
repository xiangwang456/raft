package raft

import "io"

type SnapHot struct {
	LastTerm  uint64 `json:"lastIndex"`
	LastIndex uint64 `josn:"lastTerm"`

	Peers []*Peer `json:"peers"`
	State []byte  `json:"state"`
	Path  string  `json:"path"`
}

// 服务器从 snaphot 状态启动
type SnaphotRecoveryRequest struct {
	// todo :
}

//snaphot response
type SnaphotRecoveryResponse struct {
	Term        uint64
	Success     bool
	CommitIndex uint64
}

type SnaphotRequest struct {
}

type SnaphotResponse struct {
}

func (ss *SnapHot) save() error {
	//todo : 1、打开文件 2、序列化protuuf  3、写入文件

	var err error

	return err

}

func (ss *SnapHot) remove() error {
	// todo : 1、将此文件移除
	var err error

	return err
}

func (req *SnaphotRequest) Encode(w io.Writer) (int, error) {
	// todo

	return 0, nil
}

func (req *SnaphotRequest) Decode(w io.Writer) (int, error) {
	//todo

	return 0, nil
}

func (req *SnaphotResponse) Encode(w io.Writer) (int, error) {
	// todo

	return 0, nil
}

func (req *SnaphotResponse) Decode(w io.Writer) (int, error) {
	//todo

	return 0, nil
}

func (req *SnaphotRecoveryRequest) Encode(w io.Writer) (int, error) {
	// todo

	return 0, nil
}

func (req *SnaphotRecoveryRequest) Decode(w io.Writer) (int, error) {
	//todo

	return 0, nil
}

func (req *SnaphotRecoveryResponse) Encode(w io.Writer) (int, error) {
	// todo

	return 0, nil
}

func (req *SnaphotRecoveryResponse) Decode(w io.Writer) (int, error) {
	//todo

	return 0, nil
}

func newSnapHotRecoryRequest(success bool) *SnaphotRecoveryRequest {
	return &SnaphotRecoveryRequest{}
}

func newSnapHotRecoryResponse(success bool) *SnaphotRecoveryResponse {
	return &SnaphotRecoveryResponse{Success: success}
}

func newSnapHotRequest(success bool) *SnaphotRequest {
	return &SnaphotRequest{}
}

func newSnapHotResponse(success bool) *SnaphotResponse {
	return &SnaphotResponse{}
}
