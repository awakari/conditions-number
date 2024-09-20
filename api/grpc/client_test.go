package grpc

import (
	"context"
	"fmt"
	"github.com/awakari/conditions-number/service"
	"github.com/awakari/conditions-number/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"log/slog"
	"os"
	"testing"
)

const port = 50051

var log = slog.Default()

func TestMain(m *testing.M) {
	svc := service.NewService(storage.NewStorageMock())
	svc = service.NewServiceLogging(svc, log)
	go func() {
		err := Serve(svc, port)
		if err != nil {
			log.Error(err.Error())
		}
	}()
	code := m.Run()
	os.Exit(code)
}

func TestClient_Create(t *testing.T) {
	//
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Nil(t, err)
	client := NewServiceClient(conn)
	//
	cases := map[string]struct {
		key string
		val float64
		err error
	}{
		"ok": {
			key: "key0",
			val: 42,
		},
		"fail": {
			key: "fail",
			err: status.Error(codes.Internal, "internal failure"),
		},
		"conflict": {
			key: "conflict",
			err: status.Error(codes.AlreadyExists, "already exists"),
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var resp *CreateResponse
			resp, err = client.Create(context.TODO(), &CreateRequest{
				Key: c.key,
				Op:  Operation_Gte,
				Val: c.val,
			})
			if c.err == nil {
				assert.NotEmpty(t, resp.Id)
			}
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestClient_LockCreate(t *testing.T) {
	//
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Nil(t, err)
	client := NewServiceClient(conn)
	//
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: "cond0",
		},
		"missing": {
			id:  "missing",
			err: status.Error(codes.NotFound, "not found"),
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			_, err = client.LockCreate(context.TODO(), &LockCreateRequest{
				Id: c.id,
			})
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestClient_UnlockCreate(t *testing.T) {
	//
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Nil(t, err)
	client := NewServiceClient(conn)
	//
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: "cond0",
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			_, err = client.UnlockCreate(context.TODO(), &UnlockCreateRequest{
				Id: c.id,
			})
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestClient_Delete(t *testing.T) {
	//
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Nil(t, err)
	client := NewServiceClient(conn)
	//
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: "cond0",
		},
		"fail": {
			id:  "fail",
			err: status.Error(codes.Internal, "internal failure"),
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			_, err = client.Delete(context.TODO(), &DeleteRequest{
				Id: c.id,
			})
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestClient_Search(t *testing.T) {
	//
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Nil(t, err)
	client := NewServiceClient(conn)
	//
	cases := map[string]struct {
		key string
		val float64
		ids []string
		err error
	}{
		"ok": {
			key: "",
			val: 42,
			ids: []string{
				"cond0",
				"cond1",
				"cond2",
			},
		},
		"fail": {
			key: "fail",
			val: 42,
			err: status.Error(codes.Internal, "internal failure"),
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var stream Service_SearchClient
			stream, err = client.Search(context.TODO(), &SearchRequest{
				Key: c.key,
				Val: c.val,
			})
			assert.Nil(t, err)
			var ids []string
			var resp *SearchResponse
			for {
				resp, err = stream.Recv()
				if err == io.EOF {
					err = nil
					break
				}
				if err != nil {
					break
				}
				ids = append(ids, resp.Id)
			}
			assert.Equal(t, len(c.ids), len(ids))
			assert.ElementsMatch(t, c.ids, ids)
			assert.ErrorIs(t, err, c.err)
			err = stream.CloseSend()
			assert.Nil(t, err)
		})
	}
}
