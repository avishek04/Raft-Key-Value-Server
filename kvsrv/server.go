package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type RespSeq struct {
	reqNum int64
	value  string
}

type KeyValSeq struct {
	value string
	kSeq  int64
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStore    map[string]KeyValSeq
	clntSeqMap map[int64]RespSeq
	seqNum     int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	valSeq, ok := kv.kvStore[args.Key]
	currSeqNum := kv.seqNum + 1
	if ok && currSeqNum > valSeq.kSeq {
		kv.seqNum = currSeqNum
		reply.Value = valSeq.value
		valSeq.kSeq = currSeqNum
		kv.kvStore[args.Key] = valSeq
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valSeq, okay := kv.kvStore[args.Key]
	currSeqNum := kv.seqNum + 1

	if !okay || (okay && currSeqNum > valSeq.kSeq) {
		kv.seqNum = currSeqNum
		reply.Value = valSeq.value
		valSeq.value = args.Value
		valSeq.kSeq = currSeqNum
		kv.kvStore[args.Key] = valSeq
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId := args.ClientId
	clientReqNum := args.ReqSeqNum
	storedReq, ok := kv.clntSeqMap[clientId]
	if ok && clientReqNum == storedReq.reqNum {
		reply.Value = storedReq.value
		return
	}
	valSeq, okay := kv.kvStore[args.Key]
	currSeqNum := kv.seqNum + 1
	if !okay || (okay && currSeqNum > valSeq.kSeq) {
		kv.seqNum = currSeqNum
		reply.Value = valSeq.value
		valSeq.value = valSeq.value + args.Value
		valSeq.kSeq = currSeqNum
		kv.kvStore[args.Key] = valSeq
		storedReq.reqNum = clientReqNum
		storedReq.value = reply.Value
		kv.clntSeqMap[clientId] = storedReq
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvStore = make(map[string]KeyValSeq)
	kv.clntSeqMap = make(map[int64]RespSeq)
	kv.seqNum = 1
	return kv
}
