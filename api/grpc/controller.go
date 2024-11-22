package grpc

import (
	"context"
	"errors"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/service"
	"github.com/awakari/conditions-number/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controller struct {
	svc service.Service
}

func NewController(svc service.Service) ServiceServer {
	return controller{
		svc: svc,
	}
}

func (c controller) Create(ctx context.Context, req *CreateRequest) (resp *CreateResponse, err error) {
	resp = &CreateResponse{}
	var op model.Op
	switch req.Op {
	case Operation_Gt:
		op = model.OpGt
	case Operation_Gte:
		op = model.OpGte
	case Operation_Eq:
		op = model.OpEq
	case Operation_Lte:
		op = model.OpLte
	case Operation_Lt:
		op = model.OpLt
	default:
		op = model.OpUndefined
	}
	resp.Id, err = c.svc.Create(ctx, req.Key, op, req.Val)
	err = encodeError(err)
	return
}

func (c controller) LockCreate(ctx context.Context, req *LockCreateRequest) (resp *LockCreateResponse, err error) {
	resp = &LockCreateResponse{}
	err = c.svc.LockCreate(ctx, req.Id)
	err = encodeError(err)
	return
}

func (c controller) UnlockCreate(ctx context.Context, req *UnlockCreateRequest) (resp *UnlockCreateResponse, err error) {
	resp = &UnlockCreateResponse{}
	err = c.svc.UnlockCreate(ctx, req.Id)
	err = encodeError(err)
	return
}

func (c controller) Delete(ctx context.Context, req *DeleteRequest) (resp *DeleteResponse, err error) {
	resp = &DeleteResponse{}
	err = c.svc.Delete(ctx, req.Id)
	err = encodeError(err)
	return
}

func (c controller) SearchPage(ctx context.Context, req *SearchPageRequest) (resp *SearchPageResponse, err error) {
	resp = &SearchPageResponse{}
	resp.Ids, err = c.svc.SearchPage(ctx, req.Key, req.Val, req.Limit, req.Cursor)
	err = encodeError(err)
	return
}

func encodeError(src error) (dst error) {
	switch {
	case src == nil:
		dst = nil
	case errors.Is(src, storage.ErrInternal):
		dst = status.Error(codes.Internal, src.Error())
	case errors.Is(src, storage.ErrConflict):
		dst = status.Error(codes.AlreadyExists, src.Error())
	case errors.Is(src, storage.ErrNotFound):
		dst = status.Error(codes.NotFound, src.Error())
	default:
		dst = status.Error(codes.Unknown, src.Error())
	}
	return
}
