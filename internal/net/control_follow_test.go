package net

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/drand/drand/v2/common"
	"github.com/drand/drand/v2/common/log"
	testnet "github.com/drand/drand/v2/internal/test/net"
	proto "github.com/drand/drand/v2/protobuf/drand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type followFloodServer struct {
	testnet.EmptyServer
	sent chan struct{}
}

func (s *followFloodServer) StartFollowChain(_ *proto.StartSyncRequest, stream proto.Control_StartFollowChainServer) error {
	// Send enough messages to fill the client's outCh buffer, forcing the client goroutine
	// into the select that waits on either outCh being writable or context cancellation.
	for i := 0; i < progressSyncQueue*2; i++ {
		if err := stream.Send(&proto.SyncProgress{Current: uint64(i), Target: uint64(progressSyncQueue * 2)}); err != nil {
			return err
		}
	}
	if s.sent != nil {
		close(s.sent)
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestStartFollowChain_ClosesChannelsOnCancelEvenIfCallerDoesNotConsume(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	t.Cleanup(srv.Stop)
	flood := &followFloodServer{sent: make(chan struct{})}
	proto.RegisterControlServer(srv, flood)

	go func() {
		_ = srv.Serve(lis)
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	c := &ControlClient{
		log:     log.DefaultLogger(),
		client:  proto.NewControlClient(conn),
		version: common.GetAppVersion(),
	}

	fctx, cancel := context.WithCancel(ctx)
	outCh, errCh, err := c.StartFollowChain(fctx,
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		[]string{"127.0.0.1:1234"},
		0,
		common.DefaultBeaconID,
	)
	if err != nil {
		t.Fatalf("StartFollowChain: %v", err)
	}

	select {
	case <-flood.sent:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to send progress")
	}
	cancel()

	timeout := time.After(2 * time.Second)
	for outCh != nil || errCh != nil {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for channels to close")
		case _, ok := <-outCh:
			if !ok {
				outCh = nil
			}
		case _, ok := <-errCh:
			if !ok {
				errCh = nil
			}
		}
	}
}
