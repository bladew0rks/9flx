package p9fs

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"

	"github.com/ronsor/go9p"
)

func Serve(ctx context.Context, address string, tree *Tree) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()
	var connectionMu sync.Mutex
	active := make(map[net.Conn]struct{})
	go func() {
		<-ctx.Done()
		_ = listener.Close()
		connectionMu.Lock()
		for connection := range active {
			_ = connection.Close()
		}
		connectionMu.Unlock()
	}()
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		connectionMu.Lock()
		active[connection] = struct{}{}
		connectionMu.Unlock()
		connections.Add(1)
		go func() {
			defer connections.Done()
			defer func() { connectionMu.Lock(); delete(active, connection); connectionMu.Unlock() }()
			defer connection.Close()
			_ = go9p.ServeReadWriter(bufio.NewReader(connection), connection, tree.FS.Server())
		}()
	}
}
