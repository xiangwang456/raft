package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
	"github.com/golang/protobuf/proto"
	"github.com/zoo-keeper/raft/protobuf"
	"bytes"
	"encoding/json"
	"path"
)

var (
	errTermTooSmall    = errors.New("term too small")
	errIndexTooSmall   = errors.New("index too small")
	errIndexTooBig     = errors.New("commit index too big")
	errInvalidChecksum = errors.New("invalid checksum")
	errNoCommand       = errors.New("no command")
	errBadIndex        = errors.New("bad index")
	errBadTerm         = errors.New("bad term")
)

type raftLog struct {
	sync.RWMutex
	file      *os.File
	entries    []logEntry
	commitPos  uint64
	startIndex uint64 //todo start index的更新
	startTerm  uint64 // todo start  item 的更新
	apply      func(uint64, []byte) []byte
	path       string
}

func newRaftLog(store *os.File, apply func(uint64, []byte) []byte) *raftLog {

	// 如果有文件 ，则从文件恢复，否则创建新的config
	l := &raftLog{
		store:     store,
		entries:   []logEntry{},
		commitPos: 0, // no commits to begin with
		apply:     apply,
	}
	l.recover(store)
	return l
}


func (l *raftLog) open(path string) error{
	// 从日志中读取
	var readBytes int64

	var err error

	l.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	l.path = path

	if err != nil {

		if os.IsNotExist(err) {
			l.file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
		}
		return err
	}


	for { //todo
		// Instantiate log entry and decode into it.
		entry, _ := newLogEntry(l, 0, 0, nil)
		entry.Position, _ = l.file.Seek(0, os.SEEK_CUR)

		//n, err := entry.decode(l.file)
		//if err != nil {
		//	if err == io.EOF {
		//		debugln("open.log.append: finish ")
		//	} else {
		//		if err = os.Truncate(path, readBytes); err != nil {
		//			return fmt.Errorf("raft.Log: Unable to recover: %v", err)
		//		}
		//	}
		//	break
		//}
		//if entry.Index() > l.startIndex {
		//	// Append entry.
		//	l.entries = append(l.entries, entry)
		//	if entry.Index() <= l.commitIndex {
		//		command, err := newCommand(entry.CommandName(), entry.Command())
		//		if err != nil {
		//			continue
		//		}
		//		l.ApplyFunc(entry, command)
		//	}

		}

		readBytes += int64(0)

	return nil
}


// entriesAfter returns a slice of log entries after (i.e. not including) the
// passed index, and the term of the log entry specified by index, as a
// convenience to the caller. (This function is only used by a leader attempting
// to flush log entries to its followers.)
//
// This function is called to populate an AppendEntries RPC. That implies they
// are destined for a follower, which implies the application of the commands
// should have the response thrown away, which implies we shouldn't pass a
// commandResponse channel (see: commitTo implementation). In the normal case,
// the raftLogEntries we return here will get serialized as they pass thru their
// transport, and lose their commandResponse channel anyway. But in the case of
// a LocalPeer (or equivalent) this doesn't happen. So, we must make sure to
// proactively strip commandResponse channels.
func (l *raftLog) entriesAfter(index uint64) ([]logEntry, uint64) {
	l.RLock()
	defer l.RUnlock()

	pos := 0
	lastTerm := uint64(0)
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index > index {
			break
		}
		lastTerm = l.entries[pos].Term
	}

	a := l.entries[pos:]
	if len(a) == 0 {
		return []logEntry{}, lastTerm
	}

	return stripResponseChannels(a), lastTerm
}

func stripResponseChannels(a []logEntry) []logEntry {
	stripped := make([]logEntry, len(a))
	for i, entry := range a {
		stripped[i] = logEntry{
			Index:           entry.Index,
			Term:            entry.Term,
			Command:         entry.Command,
			commandResponse: nil,
		}
	}
	return stripped
}

func (l *raftLog) compact(index uint64, term uint64) error {
	//1、找出需要压缩的日志 2、创建压缩文件 3、将信息写入文件 4、重置内存中的日志信息

	l.RLock()
	defer l.RLock()

	//1、找到需要压缩的日志
	if index == 0 || index > l.lastIndex() {
		return nil
	}

	entries := l.entries[l.startIndex:]

	//2、创建压缩文件
	newFilePah := l.path + `.new`
	file, err := os.OpenFile(newFilePah, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil { //todo what mean
		return err
	}

	//3、写入文件
	for _, entry := range entries {
		//position, _ := l.store.Seek(0,0) //todo right ?
		//entry.position = uint64(position)

		if err = entry.encode(file); err != nil {
			file.Close()
			os.Remove(newFilePah)
			return err
		}
	}

	file.Sync()

	// 4、更新日志里的信息
	old_file := l.store

	// 重命名
	err = os.Rename(newFilePah, l.path)
	if err != nil {
		file.Close()
		os.Remove(newFilePah)
		return err
	}
	l.store = file

	old_file.Close()

	// 修改内存里的日志
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	return nil

}

// contains returns true if a log entry with the given index and term exists in
// the log.
func (l *raftLog) contains(index, term uint64) bool {
	l.RLock()
	defer l.RUnlock()

	// It's not necessarily true that l.entries[i] has index == i.
	for _, entry := range l.entries {
		if entry.Index == index && entry.Term == term {
			return true
		}
		if entry.Index > index || entry.Term > term {
			break
		}
	}
	return false
}

// ensureLastIs deletes all non-committed log entries after the given index and
// term. It will fail if the given index doesn't exist, has already been
// committed, or doesn't match the given term.
//
// This method satisfies the requirement that a log entry in an AppendEntries
// call precisely follows the accompanying LastraftLogTerm and LastraftLogIndex.
// 将 index后面的没有commit的元素删掉
func (l *raftLog) ensureLastIs(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	// Taken loosely from benbjohnson's impl

	if index < l.getCommitIndexWithLock() {
		return errIndexTooSmall
	}

	if index > l.lastIndexWithLock() {
		return errIndexTooBig
	}

	// It's possible that the passed index is 0. It means the leader has come to
	// decide we need a complete log rebuild. Of course, that's only valid if we
	// haven't committed anything, so this check comes after that one.
	// 初始化logEntry
	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].commandResponse != nil {
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
			if l.entries[pos].committed != nil {
				l.entries[pos].committed <- false
				close(l.entries[pos].committed)
				l.entries[pos].committed = nil
			}
		}
		l.entries = []logEntry{}
		return nil
	}

	// Normal case: find the position of the matching log entry.
	pos := 0
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index < index {
			continue // didn't find it yet
		}
		if l.entries[pos].Index > index {
			return errBadIndex // somehow went past it
		}
		if l.entries[pos].Index != index {
			panic("not <, not >, but somehow !=")
		}
		if l.entries[pos].Term != term {
			return errBadTerm
		}
		break // good
	}

	// Sanity check.
	if pos < int(l.commitPos) {
		panic("index >= commitIndex, but pos < commitPos")
	}

	// `pos` is the position of log entry matching index and term.
	// We want to truncate everything after that.
	truncateFrom := pos + 1
	if truncateFrom >= len(l.entries) {
		return nil // nothing to truncate
	}

	// If we blow away log entries that haven't yet sent responses to clients,
	// signal the clients to stop waiting, by closing the channel without a
	// response value.
	for pos = truncateFrom; pos < len(l.entries); pos++ {
		if l.entries[pos].commandResponse != nil {
			close(l.entries[pos].commandResponse)
			l.entries[pos].commandResponse = nil
		}
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- false
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}
	}

	// Truncate the log.
	l.entries = l.entries[:truncateFrom]

	// Done.
	return nil
}

// getCommitIndex returns the commit index of the log. That is, the index of the
// last log entry which can be considered committed.
func (l *raftLog) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithLock()
}

func (l *raftLog) getCommitIndexWithLock() uint64 {
	if l.commitPos < 0 {
		return 0
	}
	if int(l.commitPos) >= len(l.entries) {
		panic(fmt.Sprintf("commitPos %d > len(l.entries) %d; bad bookkeeping in raftLog", l.commitPos, len(l.entries)))
	}
	return l.entries[l.commitPos].Index
}

func (l *raftLog) getEntry(index uint64) logEntry {
	return logEntry{} //todo
}

// lastIndex returns the index of the most recent log entry.
func (l *raftLog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithLock()
}

func (l *raftLog) lastIndexWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// lastTerm returns the term of the most recent log entry.
func (l *raftLog) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithLock()
}

func (l *raftLog) lastTermWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// appendEntry appends the passed log entry to the log. It will return an error
// if the entry's term is smaller than the log's most recent term, or if the
// entry's index is too small relative to the log's most recent entry.
func (l *raftLog) appendEntry(entry logEntry) error {
	l.Lock()
	defer l.Unlock()

	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithLock()
		if entry.Term < lastTerm {
			return errTermTooSmall
		}
		lastIndex := l.lastIndexWithLock()
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return errIndexTooSmall
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// commitTo commits all log entries up to and including the passed commitIndex.
// Commit means: synchronize the log entry to persistent storage, and call the
// state machine apply function for the log entry's command.
// 将commitIndex 以前的日志都提交，记录到 store里
func (l *raftLog) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		panic("commitTo(0)")
	}

	l.Lock()
	defer l.Unlock()

	// Reject old commit indexes
	if commitIndex < l.getCommitIndexWithLock() {
		return errIndexTooSmall
	}

	// Reject new commit indexes
	if commitIndex > l.lastIndexWithLock() {
		return errIndexTooBig
	}

	// If we've already committed to the commitIndex, great!
	if commitIndex == l.getCommitIndexWithLock() {
		return nil
	}

	// We should start committing at precisely the last commitPos + 1
	pos := l.commitPos + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	// Commit entries between our existing commit index and the passed index.
	// Remember to include the passed index.
	for {
		// Sanity checks. TODO replace with plain `for` when this is stable.
		if int(pos) >= len(l.entries) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all log entries (%d)", pos, len(l.entries)))
		}
		if l.entries[pos].Index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		// Encode the entry to persistent storage.
		if err := l.entries[pos].encode(l.store); err != nil {
			return err
		}

		// Forward non-configuration commands to the state machine.
		// Send the responses to the waiting client, if applicable.
		// 如果该日志不是配置相关日志，则调用日志的apply方法，并等待回应
		if !l.entries[pos].isConfiguration {
			resp := l.apply(l.entries[pos].Index, l.entries[pos].Command)
			if l.entries[pos].commandResponse != nil {
				select {
				case l.entries[pos].commandResponse <- resp:
					break
				case <-time.After(maximumElectionTimeout()): // << ElectionInterval
					panic("uncoöperative command response receiver")
				}
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
		}

		// Signal the entry has been committed, if applicable.
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- true
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}

		// Mark our commit position cursor.
		l.commitPos = pos

		// If that was the last one, we're done.
		if l.entries[pos].Index == commitIndex {
			break
		}
		if l.entries[pos].Index > commitIndex {
			panic(fmt.Sprintf(
				"current entry Index %d is beyond our desired commitIndex %d",
				l.entries[pos].Index,
				commitIndex,
			))
		}

		// Otherwise, advance!
		pos++
	}

	// Done.
	return nil
}

// logEntry is the atomic unit being managed by the distributed log. A log entry
// always has an index (monotonically increasing), a term in which the Raft
// network leader first sees the entry, and a command. The command is what gets
// executed against the node state machine when the log entry is successfully
// replicated.
type logEntry struct {
	pb       *protobuf.LogEntry
	Position int64 // 文件中的位置
	log      *raftLog


}

func newLogEntry(log *raftLog , index uint64, term uint64, command []byte) (*logEntry, error) {

	pb := &protobuf.LogEntry{
		Index:       index,
		Term:        term,
		Command:     command,
	}

	e := &logEntry{
		pb:    pb,
		log:   log,
	}

	return e, nil
}


func (e *logEntry) encode(w io.Writer) (int,error) {
	// 由原来的自定义的编码方式换成 proto todo

	b, err := proto.Marshal(e.pb)
	if err != nil {
		return -1, err
	}

	if _, err = fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return -1, err
	}

	return w.Write(b)
}

// decode deserializes one log entry from the passed io.Reader.
// 解码类型 ，先crc校验，再解析出term 、index 、command
// 改为protobuf
func (e *logEntry) decode(r io.Reader) (int,error) {

	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(r, data)

	if err != nil {
		return -1, err
	}

	if err = proto.Unmarshal(data, e.pb); err != nil {
		return -1, err
	}

	return length + 8 + 1, nil
}
