package server

import (
	"context"

	"alarm_monitor/business"
)

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc
	Kafkas []business.Consumer
}

func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		ctx:    ctx,
		cancel: cancel,
		Kafkas: make([]business.Consumer, 0, 1),
	}
}

func (this *Server) Ctx() context.Context {
	return this.ctx
}

func (this *Server) Start() error {
	for _, v := range this.Kafkas {
		if err := v.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (this *Server) Stop() {
	for _, v := range this.Kafkas {
		v.Stop()
	}
}
