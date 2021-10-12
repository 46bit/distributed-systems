package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/46bit/distributed-systems/ticket-issuer-api/api"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TicketIssuerServer struct {
	api.UnimplementedTicketIssuerServer

	config    *Config
	startTime *time.Time

	redisClientsLock sync.RWMutex
	redisClients     map[*redis.Options]*redis.Client
}

var _ api.TicketIssuerServer = (*TicketIssuerServer)(nil)

func NewTicketIssuerServer(config *Config) *TicketIssuerServer {
	now := time.Now()
	return &TicketIssuerServer{
		config:       config,
		startTime:    &now,
		redisClients: map[*redis.Options]*redis.Client{},
	}
}

func (s *TicketIssuerServer) Health(ctx context.Context, _ *api.HealthRequest) (*api.HealthResponse, error) {
	return &api.HealthResponse{
		Uptime: durationpb.New(s.uptime()),
	}, nil
}

func (s *TicketIssuerServer) IssueTicket(ctx context.Context, req *api.IssueTicketRequest) (*api.IssueTicketResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	redisClient, err := s.redisClient(req.UserId)
	if err != nil {
		return nil, err
	}

	resp := &api.IssueTicketResponse{Ticketed: false}

	ticketRequestedByUserIdKey := fmt.Sprintf("ticket_requested_by_user_id_%d", req.UserId)

	// Redis stores a ticket request counter. Can be shard-specific, so long as user IDs are pinned to one shard.
	// We prevent each user ID from getting multiple tickets by implementing a lock in Redis.
	// The ticket request counter can only be incremented once per user id.
	// We issue a ticket if the pre-increment value of the ticket request counter was lower than the number of tickets.
	//
	// A good property of this system is that we aren't trying to lock a global counter. Incrementing is atomic.
	// An earlier approach tried to optimistically lock the ticket request counter. That performed terribly under load,
	// as there was huge contention. That was fixed by my insight that we can blindly increment the counter, and
	// check afterwards whether we should issue a ticket or not.
	//
	// How it works:
	//
	// 1. WATCH ticket_requested_by_user_id_$USERID		[ abort transaction if ticket_requested_by_user_id_$USERID is written externally ]
	//    GET ticket_requested_by_user_id_$USERID
	// 2. *app checks "ticket_requested_by_user_id_$USERID" did not exist, and aborts if it already did*
	// 3. MULTI											[ run all of following commands, or none at all (WATCH still applies) ]
	//    SET ticket_requested_by_user_id_$USERID yes	[ lock future attempts to get a ticket for that user id ]
	//    INCR ticket_request_counter					[ to increment ticket request counter ]
	//    EXEC											[ commit transaction ]
	// 4. *app issues user a ticket if incremented value of ticket request counter is no more than the number of tickets*
	userAlreadyRequestedTicketErr := fmt.Errorf("user already requested ticket")
	noTicketsLeftErr := fmt.Errorf("ticket_request_counter too high")
	ticketTx := func(tx *redis.Tx) error {
		_, err := tx.Get(ctx, ticketRequestedByUserIdKey).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if err != redis.Nil {
			return userAlreadyRequestedTicketErr
		}

		pipe := tx.TxPipeline()
		incr := pipe.Incr(ctx, "ticket_request_counter")
		pipe.Set(ctx, ticketRequestedByUserIdKey, "yes", 0)

		_, err = pipe.Exec(ctx)
		noTicketsLeft := incr.Val()
		if noTicketsLeft > s.config.MaxTicketsPerRedisShard {
			return noTicketsLeftErr
		}
		return err
	}

	err = redisClient.Watch(ctx, ticketTx, ticketRequestedByUserIdKey)
	if err == userAlreadyRequestedTicketErr {
		//log.Println("rejected duplicate ticket request for user id", req.UserId)
		return resp, nil
	}
	if err == noTicketsLeftErr {
		log.Println("no tickets left for user id", req.UserId)
		return resp, nil
	}
	// Treat transaction failures as failures to get tickets; they generally mean another ticket
	// was issued to the same user during the transaction
	if err == redis.TxFailedErr {
		return resp, nil
	}
	// FIXME: Possibly check for "redis.TxFailedErr". That *should* indicate that another
	// request altered ticketRequestedByUserIdKey, and be a normal reject, but I don't know
	// for sure that it can't be caused by something else.
	if err != nil {
		err = fmt.Errorf("unexpected error in redis transaction: %w", err)
		log.Println(err)
		return nil, err
	}

	resp.Ticketed = true
	fmt.Println("ticketed", req.UserId)
	return resp, nil
}

func (s *TicketIssuerServer) redisClient(userId uint64) (*redis.Client, error) {
	redisConfig := s.config.GetRedisShard(userId)
	if redisConfig == nil {
		return nil, fmt.Errorf("no shard found")
	}

	s.redisClientsLock.RLock()
	redisClient, ok := s.redisClients[redisConfig]
	s.redisClientsLock.RUnlock()
	if !ok {
		redisClient = redis.NewClient(redisConfig)
		s.redisClientsLock.Lock()
		s.redisClients[redisConfig] = redisClient
		s.redisClientsLock.Unlock()
	}
	return redisClient, nil
}

func (s *TicketIssuerServer) uptime() time.Duration {
	uptime := time.Duration(0)
	if s.startTime != nil {
		uptime = time.Now().Sub(*s.startTime)
	}
	return uptime
}
