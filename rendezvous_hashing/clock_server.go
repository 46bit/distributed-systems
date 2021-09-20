package rendezvous_hashing

import (
	"context"
	"sync"
	"fmt"
	"strconv"
	"os"
	"sync/atomic"

	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
)

// Based on http://rystsov.info/2018/10/01/tso.html
type ClockServer struct {
	api.UnimplementedClockServer

	epochPath string
	// FIXME: Heavily read workload; switch to read-write
	epochMutex sync.Mutex
	epoch uint64

	clock uint64
}

var _ api.ClockServer = (*ClockServer)(nil)

func NewClockServer(epochPath string) (*ClockServer, error) {
	epoch, err := createOrIncrementEpochFile(epochPath)
	if err != nil {
		return nil, fmt.Errorf("error initialising clock server: %w", err)
	}
	return &ClockServer{
		epochPath: epochPath,
		epoch: epoch,
		clock: 1,
	}, nil
}

func (s *ClockServer) Get(ctx context.Context, req *api.ClockGetRequest) (*api.ClockGetResponse, error) {
	s.epochMutex.Lock()
	value := &api.ClockValue{Epoch: s.epoch}
	s.epochMutex.Unlock()
	value.Clock = atomic.LoadUint64(&s.clock)

	return &api.ClockGetResponse{
		Value: value,
	}, nil
}

func (s *ClockServer) Set(ctx context.Context, req *api.ClockSetRequest) (*api.ClockSetResponse, error) {
	s.epochMutex.Lock()
	defer s.epochMutex.Unlock()

	if req.Value.Epoch > s.epoch {
		s.epoch = req.Value.Epoch
		fmt.Println("epoch is now", s.epoch)
		// FIXME: If the clock is broken, the server should probably shut down?
		if err := setEpochFile(s.epochPath, s.epoch); err != nil {
			return nil, err
		}
		atomic.StoreUint64(&s.clock, req.Value.Clock)
		fmt.Println("clock is now", s.clock)
	} else if req.Value.Epoch == s.epoch {
		// FIXME: Put a read-write lock on the whole clock object, it's not worth breaking
		// it down like this mess
		currentClock := atomic.LoadUint64(&s.clock)
		if req.Value.Clock > currentClock {
			// FIXME: Need to do a check-and-store to be correct, but see above notes about
			// simplifying sync design!!
			atomic.StoreUint64(&s.clock, req.Value.Clock)
			fmt.Println("clock is now", s.clock)
		}
	}
	// FIXME: How to handle clock being decreased? OK if epoch increased?
	return &api.ClockSetResponse{}, nil
}

// FIXME: Should lock the epoch file throughout program execution
// Easiest to wait until https://github.com/golang/go/issues/33974
func createOrIncrementEpochFile(epochPath string) (uint64, error) {
	if _, err := os.Stat(epochPath); os.IsNotExist(err) {
		epoch := uint64(1)
		return epoch, setEpochFile(epochPath, epoch)
	}

	bytes, err := os.ReadFile(epochPath)
	if err != nil {
		return 0, fmt.Errorf("error reading epoch file: %w", err)
	}

	epoch, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing epoch file: %w", err)
	}

	epoch += 1
	if err = setEpochFile(epochPath, epoch); err != nil {
		return 0, fmt.Errorf("unable to increment epoch: %w", err)
	}

	return epoch, nil
}

func setEpochFile(epochPath string, epoch uint64) error {
	return os.WriteFile(epochPath, []byte(fmt.Sprintf("%d", epoch)), 0644)
}
