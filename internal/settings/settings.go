package settings

import (
	"log"
	"sync"

	"github.com/spf13/viper"
)

var (
	instance *Config
	once     sync.Once
)

type Config struct {
}

func GetConfig() *Config {
	once.Do(func() {
		viper.AddConfigPath("../../config")
		viper.SetConfigType("yaml")
		viper.SetConfigName("cfg")
		err := viper.ReadInConfig()
		if err != nil {
			log.Fatal(err)
		}
		instance = &Config{}
	})
	return instance
}

func (c *Config) Get(key string) any {
	return viper.Get(key)
}

func (c *Config) GetString(key string) string {
	return viper.GetString(key)
}

func (c *Config) GetInt(key string) int {
	return viper.GetInt(key)
}
