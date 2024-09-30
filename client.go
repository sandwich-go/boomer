package boomer

import "context"

type client interface {
	connect(ctx context.Context) (err error)
	close()
	recvChannel() chan message
	sendChannel() chan message
	disconnectedChannel() chan bool
}
