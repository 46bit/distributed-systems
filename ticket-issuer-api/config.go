package main

import (
	"fmt"
	"io/ioutil"

	"github.com/go-redis/redis/v8"
	"gopkg.in/yaml.v2"
)

type Config struct {
	BindAddress             string
	MaxTicketsPerRedisShard int64
	RedisShards             []*redis.Options
}

func (r *Config) GetRedisShard(userId uint64) *redis.Options {
	shardIndex := userId % uint64(len(r.RedisShards))
	return r.RedisShards[shardIndex]
}

func LoadConfig(path string) (*Config, error) {
	var config struct {
		BindAddress             string   `yaml:"bind_address"`
		MaxTicketsPerRedisShard int64    `yaml:"max_tickets_per_redis_shard"`
		RedisShardUrls          []string `yaml:"redis_shard_urls"`
	}
	yamlBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}
	if err = yaml.Unmarshal(yamlBytes, &config); err != nil {
		return nil, fmt.Errorf("error deserialising config file: %w", err)
	}
	if len(config.RedisShardUrls) == 0 {
		return nil, fmt.Errorf("config must specify at least one redis shard")
	}

	var parsedConfig Config
	parsedConfig.BindAddress = config.BindAddress
	parsedConfig.MaxTicketsPerRedisShard = config.MaxTicketsPerRedisShard
	for _, redisShardUrl := range config.RedisShardUrls {
		opt, err := redis.ParseURL(redisShardUrl)
		if err != nil {
			return nil, err
		}
		parsedConfig.RedisShards = append(parsedConfig.RedisShards, opt)
	}
	return &parsedConfig, nil
}
