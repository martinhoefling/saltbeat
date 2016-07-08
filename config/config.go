// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Saltbeat SaltbeatConfig
}

type SaltbeatConfig struct {
	MasterEventPub string `config:"master_event_pub"`
}
