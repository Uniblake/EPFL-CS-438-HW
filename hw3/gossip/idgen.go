package gossip

// UniqueIDGenerator describes a primitive to generate unique monotonically
// increasing sequence number for proposers
type UniqueIDGenerator interface {
	GetNext() int
}

// Use this sequence generator to generate uniq monotonically increasing IDs for
// Paxos proposers.

type uniqIDGen struct {
	currentID int
	total     int
}

func newSeqGen(id, total int) *uniqIDGen {
	return &uniqIDGen{
		currentID: id,
		total:     total,
	}
}

func (s *uniqIDGen) GetNext() int {
	res := s.currentID
	s.currentID = s.currentID + s.total

	return res
}

func (s *uniqIDGen) GetCurrent() int {
	return s.currentID
}

func (s *uniqIDGen) Reset() {
	s.currentID = s.currentID % s.total
}
