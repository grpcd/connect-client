//revive:disable:package-comments
package client

// Configuration of the client connection to grpcd.
type Configuration struct {
	GRPCDAddress string `env:"GRPCD_ADDRESS"`
}
